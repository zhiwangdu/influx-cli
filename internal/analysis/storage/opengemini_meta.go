package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type openGeminiMetaTopology struct {
	Term            uint64
	Index           uint64
	ClusterID       uint64
	ClusterPtNum    uint64
	PtNumPerNode    uint64
	IsSQLiteEnabled bool
	DataNodes       []openGeminiMetaNode
	MetaNodes       []openGeminiMetaNode
	SQLNodes        []openGeminiMetaNode
	Databases       []openGeminiMetaDatabase
	PtView          map[string][]openGeminiMetaPtInfo
	ReplicaGroups   map[string]int
	Encoding        string
}

type openGeminiMetaNode struct {
	ID              uint64
	Host            string
	TCPHost         string
	RPCAddr         string
	GossipAddr      string
	Status          int64
	LTime           uint64
	Role            string
	AZ              string
	ConnID          uint64
	AliveConnID     uint64
	SegregateStatus uint64
}

type openGeminiMetaDatabase struct {
	Name                   string
	DefaultRetentionPolicy string
	ReplicaN               int64
	MarkDeleted            bool
	RetentionPolicies      []openGeminiMetaRetentionPolicy
}

type openGeminiMetaRetentionPolicy struct {
	Name               string
	Duration           int64
	ShardGroupDuration int64
	ReplicaN           uint64
	MarkDeleted        bool
	MeasurementCount   int
	ShardGroupCount    int
	IndexGroupCount    int
}

type openGeminiMetaPtInfo struct {
	PtID        uint64
	OwnerNodeID uint64
	Status      uint64
	Version     uint64
	RGID        uint64
}

func analyzeOpenGeminiMeta(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("opengemini-meta format requires a topology snapshot file, got directory %s", filepath.Base(path))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return FileReport{}, err
	}
	topology, err := parseOpenGeminiMetaTopology(data)
	if err != nil {
		return FileReport{}, err
	}
	dbCount, rpCount, ptViewCount, ptCount, rgCount := summarizeOpenGeminiMetaTopology(topology)
	report := FileReport{
		Path:         path,
		Format:       FormatOpenGeminiMeta,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime(),
		KeyCount:     dbCount,
		BlockCount:   dbCount + rpCount + len(topology.DataNodes) + len(topology.MetaNodes) + len(topology.SQLNodes) + ptCount + rgCount,
		BlocksByType: map[string]int{},
		Extra: map[string]string{
			"encoding":           topology.Encoding,
			"layout":             "opengemini-meta-topology",
			"term":               fmt.Sprint(topology.Term),
			"index":              fmt.Sprint(topology.Index),
			"cluster_id":         fmt.Sprint(topology.ClusterID),
			"cluster_pt_num":     fmt.Sprint(topology.ClusterPtNum),
			"pt_num_per_node":    fmt.Sprint(topology.PtNumPerNode),
			"sqlite_enabled":     fmt.Sprint(topology.IsSQLiteEnabled),
			"databases":          fmt.Sprint(dbCount),
			"retention_policies": fmt.Sprint(rpCount),
			"meta_nodes":         fmt.Sprint(len(topology.MetaNodes)),
			"data_nodes":         fmt.Sprint(len(topology.DataNodes)),
			"sql_nodes":          fmt.Sprint(len(topology.SQLNodes)),
			"pt_views":           fmt.Sprint(ptViewCount),
			"pts":                fmt.Sprint(ptCount),
			"replica_groups":     fmt.Sprint(rgCount),
		},
	}
	if dbCount > 0 {
		report.BlocksByType["database"] = dbCount
	}
	if rpCount > 0 {
		report.BlocksByType["retention-policy"] = rpCount
	}
	if len(topology.MetaNodes) > 0 {
		report.BlocksByType["meta-node"] = len(topology.MetaNodes)
	}
	if len(topology.DataNodes) > 0 {
		report.BlocksByType["data-node"] = len(topology.DataNodes)
	}
	if len(topology.SQLNodes) > 0 {
		report.BlocksByType["sql-node"] = len(topology.SQLNodes)
	}
	if ptCount > 0 {
		report.BlocksByType["pt"] = ptCount
	}
	if rgCount > 0 {
		report.BlocksByType["replica-group"] = rgCount
	}
	populateOpenGeminiMetaSamples(&report, topology, options.KeySampleLimit, options.BlockSampleLimit)
	return report, nil
}

func summarizeOpenGeminiMetaTopology(topology openGeminiMetaTopology) (int, int, int, int, int) {
	rpCount := 0
	for _, db := range topology.Databases {
		rpCount += len(db.RetentionPolicies)
	}
	ptCount := 0
	for _, pts := range topology.PtView {
		ptCount += len(pts)
	}
	rgCount := 0
	for _, count := range topology.ReplicaGroups {
		rgCount += count
	}
	return len(topology.Databases), rpCount, len(topology.PtView), ptCount, rgCount
}

func populateOpenGeminiMetaSamples(report *FileReport, topology openGeminiMetaTopology, keyLimit int, blockLimit int) {
	for _, db := range sortedOpenGeminiMetaDatabases(topology.Databases) {
		if len(report.KeySamples) < keyLimit {
			report.KeySamples = append(report.KeySamples, "db:"+db.Name)
		}
		if len(report.Blocks) < blockLimit {
			report.Blocks = append(report.Blocks, BlockReport{Key: db.Name, Type: "database", ValueCount: len(db.RetentionPolicies)})
		}
		for _, rp := range sortedOpenGeminiMetaRetentionPolicies(db.RetentionPolicies) {
			if len(report.KeySamples) < keyLimit {
				report.KeySamples = append(report.KeySamples, fmt.Sprintf("rp:%s/%s", db.Name, rp.Name))
			}
			if len(report.Blocks) < blockLimit {
				report.Blocks = append(report.Blocks, BlockReport{
					Key:             fmt.Sprintf("%s/%s", db.Name, rp.Name),
					Type:            "retention-policy",
					ValueCount:      rp.MeasurementCount,
					ContainedChunks: rp.ShardGroupCount + rp.IndexGroupCount,
				})
			}
		}
	}
	for _, node := range sortedOpenGeminiMetaNodes(topology.DataNodes) {
		if len(report.KeySamples) < keyLimit {
			report.KeySamples = append(report.KeySamples, fmt.Sprintf("data-node:%d@%s", node.ID, node.Host))
		}
		if len(report.Blocks) < blockLimit {
			report.Blocks = append(report.Blocks, BlockReport{Key: strconv.FormatUint(node.ID, 10), Type: "data-node"})
		}
	}
	for _, node := range sortedOpenGeminiMetaNodes(topology.SQLNodes) {
		if len(report.KeySamples) < keyLimit {
			report.KeySamples = append(report.KeySamples, fmt.Sprintf("sql-node:%d@%s", node.ID, node.Host))
		}
		if len(report.Blocks) < blockLimit {
			report.Blocks = append(report.Blocks, BlockReport{Key: strconv.FormatUint(node.ID, 10), Type: "sql-node"})
		}
	}
	for _, db := range sortedOpenGeminiMetaPtViewNames(topology.PtView) {
		pts := append([]openGeminiMetaPtInfo(nil), topology.PtView[db]...)
		sort.Slice(pts, func(i, j int) bool { return pts[i].PtID < pts[j].PtID })
		for _, pt := range pts {
			if len(report.KeySamples) < keyLimit {
				report.KeySamples = append(report.KeySamples, fmt.Sprintf("pt:%s/%d->node:%d", db, pt.PtID, pt.OwnerNodeID))
			}
			if len(report.Blocks) < blockLimit {
				report.Blocks = append(report.Blocks, BlockReport{
					Key:      fmt.Sprintf("%s/%d", db, pt.PtID),
					SeriesID: pt.OwnerNodeID,
					Type:     "pt",
				})
			}
		}
	}
}

func sortedOpenGeminiMetaDatabases(databases []openGeminiMetaDatabase) []openGeminiMetaDatabase {
	out := append([]openGeminiMetaDatabase(nil), databases...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortedOpenGeminiMetaRetentionPolicies(policies []openGeminiMetaRetentionPolicy) []openGeminiMetaRetentionPolicy {
	out := append([]openGeminiMetaRetentionPolicy(nil), policies...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func sortedOpenGeminiMetaNodes(nodes []openGeminiMetaNode) []openGeminiMetaNode {
	out := append([]openGeminiMetaNode(nil), nodes...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sortedOpenGeminiMetaPtViewNames(views map[string][]openGeminiMetaPtInfo) []string {
	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func parseOpenGeminiMetaTopology(data []byte) (openGeminiMetaTopology, error) {
	if len(data) == 0 {
		return openGeminiMetaTopology{}, fmt.Errorf("empty openGemini meta snapshot")
	}
	trimmed := strings.TrimSpace(string(data[:openGeminiMetaMinInt(len(data), 64)]))
	if strings.HasPrefix(trimmed, "{") {
		topology, err := parseOpenGeminiMetaJSON(data)
		if err != nil {
			return openGeminiMetaTopology{}, err
		}
		topology.Encoding = "json"
		return topology, nil
	}
	topology, err := parseOpenGeminiMetaProto(data)
	if err != nil {
		return openGeminiMetaTopology{}, err
	}
	topology.Encoding = "protobuf"
	return topology, nil
}

func parseOpenGeminiMetaProto(data []byte) (openGeminiMetaTopology, error) {
	var topology openGeminiMetaTopology
	topology.PtView = map[string][]openGeminiMetaPtInfo{}
	topology.ReplicaGroups = map[string]int{}
	err := forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireVarint {
				topology.Term = value
			}
		case 2:
			if wire == protoWireVarint {
				topology.Index = value
			}
		case 3:
			if wire == protoWireVarint {
				topology.ClusterID = value
			}
		case 5:
			if wire == protoWireBytes {
				topology.Databases = append(topology.Databases, parseOpenGeminiMetaDatabaseProto(payload))
			}
		case 10:
			if wire == protoWireBytes {
				topology.DataNodes = append(topology.DataNodes, parseOpenGeminiMetaDataNodeProto(payload))
			}
		case 11:
			if wire == protoWireBytes {
				topology.MetaNodes = append(topology.MetaNodes, parseOpenGeminiMetaNodeProto(payload))
			}
		case 14:
			if wire == protoWireVarint {
				topology.ClusterPtNum = value
			}
		case 15:
			if wire == protoWireBytes {
				db, pts := parseOpenGeminiMetaPtViewEntryProto(payload)
				if db != "" {
					topology.PtView[db] = pts
				}
			}
		case 16:
			if wire == protoWireVarint {
				topology.PtNumPerNode = value
			}
		case 28:
			if wire == protoWireBytes {
				db, count := parseOpenGeminiMetaReplicaGroupEntryProto(payload)
				if db != "" {
					topology.ReplicaGroups[db] = count
				}
			}
		case 32:
			if wire == protoWireVarint {
				topology.IsSQLiteEnabled = value != 0
			}
		case 33:
			if wire == protoWireBytes {
				topology.SQLNodes = append(topology.SQLNodes, parseOpenGeminiMetaDataNodeProto(payload))
			}
		}
		return nil
	})
	if err != nil {
		return openGeminiMetaTopology{}, err
	}
	if !looksLikeOpenGeminiMetaTopology(topology) {
		return openGeminiMetaTopology{}, fmt.Errorf("not an openGemini meta topology snapshot")
	}
	return topology, nil
}

func looksLikeOpenGeminiMetaTopology(topology openGeminiMetaTopology) bool {
	return topology.Term != 0 || topology.Index != 0 || topology.ClusterID != 0 || len(topology.Databases) > 0 || len(topology.DataNodes) > 0 || len(topology.MetaNodes) > 0 || len(topology.SQLNodes) > 0 || len(topology.PtView) > 0
}

func parseOpenGeminiMetaDatabaseProto(data []byte) openGeminiMetaDatabase {
	var db openGeminiMetaDatabase
	_ = forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				db.Name = string(payload)
			}
		case 2:
			if wire == protoWireBytes {
				db.DefaultRetentionPolicy = string(payload)
			}
		case 3:
			if wire == protoWireBytes {
				db.RetentionPolicies = append(db.RetentionPolicies, parseOpenGeminiMetaRetentionPolicyProto(payload))
			}
		case 5:
			if wire == protoWireVarint {
				db.MarkDeleted = value != 0
			}
		case 8:
			if wire == protoWireVarint {
				db.ReplicaN = int64(value)
			}
		}
		return nil
	})
	return db
}

func parseOpenGeminiMetaRetentionPolicyProto(data []byte) openGeminiMetaRetentionPolicy {
	var rp openGeminiMetaRetentionPolicy
	_ = forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				rp.Name = string(payload)
			}
		case 2:
			if wire == protoWireVarint {
				rp.Duration = int64(value)
			}
		case 3:
			if wire == protoWireVarint {
				rp.ShardGroupDuration = int64(value)
			}
		case 4:
			if wire == protoWireVarint {
				rp.ReplicaN = value
			}
		case 5:
			if wire == protoWireBytes {
				rp.MeasurementCount++
			}
		case 6:
			if wire == protoWireBytes {
				rp.ShardGroupCount++
			}
		case 8:
			if wire == protoWireVarint {
				rp.MarkDeleted = value != 0
			}
		case 12:
			if wire == protoWireBytes {
				rp.IndexGroupCount++
			}
		}
		return nil
	})
	return rp
}

func parseOpenGeminiMetaDataNodeProto(data []byte) openGeminiMetaNode {
	var node openGeminiMetaNode
	_ = forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				node = parseOpenGeminiMetaNodeProto(payload)
			}
		case 2:
			if wire == protoWireVarint {
				node.ConnID = value
			}
		case 3:
			if wire == protoWireVarint {
				node.AliveConnID = value
			}
		case 4:
			if wire == protoWireBytes {
				node.AZ = string(payload)
			}
		}
		return nil
	})
	return node
}

func parseOpenGeminiMetaNodeProto(data []byte) openGeminiMetaNode {
	var node openGeminiMetaNode
	_ = forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireVarint {
				node.ID = value
			}
		case 2:
			if wire == protoWireBytes {
				node.Host = string(payload)
			}
		case 3:
			if wire == protoWireBytes {
				node.TCPHost = string(payload)
			}
		case 4:
			if wire == protoWireVarint {
				node.Status = int64(value)
			}
		case 5:
			if wire == protoWireBytes {
				node.RPCAddr = string(payload)
			}
		case 6:
			if wire == protoWireVarint {
				node.LTime = value
			}
		case 7:
			if wire == protoWireBytes {
				node.GossipAddr = string(payload)
			}
		case 8:
			if wire == protoWireVarint {
				node.SegregateStatus = value
			}
		case 10:
			if wire == protoWireBytes {
				node.Role = string(payload)
			}
		}
		return nil
	})
	return node
}

func parseOpenGeminiMetaPtViewEntryProto(data []byte) (string, []openGeminiMetaPtInfo) {
	var db string
	var pts []openGeminiMetaPtInfo
	_ = forEachProtoField(data, func(field int, wire int, _ uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				db = string(payload)
			}
		case 2:
			if wire == protoWireBytes {
				pts = parseOpenGeminiMetaDBPtInfoProto(payload)
			}
		}
		return nil
	})
	return db, pts
}

func parseOpenGeminiMetaDBPtInfoProto(data []byte) []openGeminiMetaPtInfo {
	var pts []openGeminiMetaPtInfo
	_ = forEachProtoField(data, func(field int, wire int, _ uint64, payload []byte) error {
		if field == 1 && wire == protoWireBytes {
			pts = append(pts, parseOpenGeminiMetaPtInfoProto(payload))
		}
		return nil
	})
	return pts
}

func parseOpenGeminiMetaPtInfoProto(data []byte) openGeminiMetaPtInfo {
	var pt openGeminiMetaPtInfo
	_ = forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				pt.OwnerNodeID = parseOpenGeminiMetaPtOwnerProto(payload)
			}
		case 2:
			if wire == protoWireVarint {
				pt.Status = value
			}
		case 3:
			if wire == protoWireVarint {
				pt.PtID = value
			}
		case 4:
			if wire == protoWireVarint {
				pt.Version = value
			}
		case 5:
			if wire == protoWireVarint {
				pt.RGID = value
			}
		}
		return nil
	})
	return pt
}

func parseOpenGeminiMetaPtOwnerProto(data []byte) uint64 {
	var nodeID uint64
	_ = forEachProtoField(data, func(field int, wire int, value uint64, _ []byte) error {
		if field == 1 && wire == protoWireVarint {
			nodeID = value
		}
		return nil
	})
	return nodeID
}

func parseOpenGeminiMetaReplicaGroupEntryProto(data []byte) (string, int) {
	var db string
	count := 0
	_ = forEachProtoField(data, func(field int, wire int, _ uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				db = string(payload)
			}
		case 2:
			if wire == protoWireBytes {
				count = countOpenGeminiMetaReplicaGroupsProto(payload)
			}
		}
		return nil
	})
	return db, count
}

func countOpenGeminiMetaReplicaGroupsProto(data []byte) int {
	count := 0
	_ = forEachProtoField(data, func(field int, wire int, _ uint64, _ []byte) error {
		if field == 1 && wire == protoWireBytes {
			count++
		}
		return nil
	})
	return count
}

type openGeminiMetaJSON struct {
	Term              uint64                                  `json:"Term"`
	Index             uint64                                  `json:"Index"`
	ClusterID         uint64                                  `json:"ClusterID"`
	ClusterPtNum      uint64                                  `json:"ClusterPtNum"`
	PtNumPerNode      uint64                                  `json:"PtNumPerNode"`
	IsSQLiteEnabled   bool                                    `json:"IsSQLiteEnabled"`
	DataNodes         []openGeminiMetaJSONDataNode            `json:"DataNodes"`
	MetaNodes         []openGeminiMetaJSONNode                `json:"MetaNodes"`
	SQLNodes          []openGeminiMetaJSONDataNode            `json:"SqlNodes"`
	Databases         map[string]openGeminiMetaJSONDatabase   `json:"Databases"`
	PtView            map[string][]openGeminiMetaJSONPtInfo   `json:"PtView"`
	ReplicaGroups     map[string][]map[string]json.RawMessage `json:"ReplicaGroups"`
	DatabasesArray    []openGeminiMetaJSONDatabase            `json:"databases"`
	DataNodesArray    []openGeminiMetaJSONDataNode            `json:"dataNodes"`
	MetaNodesArray    []openGeminiMetaJSONNode                `json:"metaNodes"`
	SQLNodesArray     []openGeminiMetaJSONDataNode            `json:"sqlNodes"`
	PtViewObject      map[string][]openGeminiMetaJSONPtInfo   `json:"ptView"`
	ReplicaGroupsData map[string][]map[string]json.RawMessage `json:"replicaGroups"`
}

type openGeminiMetaJSONDataNode struct {
	openGeminiMetaJSONNode
	Ni          *openGeminiMetaJSONNode `json:"Ni"`
	NodeInfo    *openGeminiMetaJSONNode `json:"NodeInfo"`
	ConnID      uint64                  `json:"ConnID"`
	AliveConnID uint64                  `json:"AliveConnID"`
	AZ          string                  `json:"Az"`
}

type openGeminiMetaJSONNode struct {
	ID              uint64 `json:"ID"`
	Host            string `json:"Host"`
	TCPHost         string `json:"TCPHost"`
	RPCAddr         string `json:"RPCAddr"`
	GossipAddr      string `json:"GossipAddr"`
	Status          int64  `json:"Status"`
	LTime           uint64 `json:"LTime"`
	Role            string `json:"Role"`
	SegregateStatus uint64 `json:"SegregateStatus"`
}

type openGeminiMetaJSONDatabase struct {
	Name                   string                                       `json:"Name"`
	DefaultRetentionPolicy string                                       `json:"DefaultRetentionPolicy"`
	ReplicaN               int64                                        `json:"ReplicaN"`
	MarkDeleted            bool                                         `json:"MarkDeleted"`
	RetentionPolicies      map[string]openGeminiMetaJSONRetentionPolicy `json:"RetentionPolicies"`
	RetentionPoliciesArray []openGeminiMetaJSONRetentionPolicy          `json:"retentionPolicies"`
}

type openGeminiMetaJSONRetentionPolicy struct {
	Name               string            `json:"Name"`
	Duration           int64             `json:"Duration"`
	ShardGroupDuration int64             `json:"ShardGroupDuration"`
	ReplicaN           uint64            `json:"ReplicaN"`
	MarkDeleted        bool              `json:"MarkDeleted"`
	Measurements       json.RawMessage   `json:"Measurements"`
	ShardGroups        []json.RawMessage `json:"ShardGroups"`
	IndexGroups        []json.RawMessage `json:"IndexGroups"`
}

type openGeminiMetaJSONPtInfo struct {
	Owner  openGeminiMetaJSONPtOwner `json:"Owner"`
	Status uint64                    `json:"Status"`
	PtID   uint64                    `json:"PtId"`
	PtID2  uint64                    `json:"PtID"`
	Ver    uint64                    `json:"Ver"`
	RGID   uint64                    `json:"RGID"`
}

type openGeminiMetaJSONPtOwner struct {
	NodeID uint64 `json:"NodeID"`
}

func parseOpenGeminiMetaJSON(data []byte) (openGeminiMetaTopology, error) {
	var raw openGeminiMetaJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return openGeminiMetaTopology{}, fmt.Errorf("decode openGemini meta json: %w", err)
	}
	topology := openGeminiMetaTopology{
		Term:            raw.Term,
		Index:           raw.Index,
		ClusterID:       raw.ClusterID,
		ClusterPtNum:    raw.ClusterPtNum,
		PtNumPerNode:    raw.PtNumPerNode,
		IsSQLiteEnabled: raw.IsSQLiteEnabled,
		PtView:          map[string][]openGeminiMetaPtInfo{},
		ReplicaGroups:   map[string]int{},
	}
	for _, node := range append(raw.DataNodes, raw.DataNodesArray...) {
		topology.DataNodes = append(topology.DataNodes, openGeminiMetaNodeFromJSONDataNode(node))
	}
	for _, node := range append(raw.MetaNodes, raw.MetaNodesArray...) {
		topology.MetaNodes = append(topology.MetaNodes, openGeminiMetaNodeFromJSON(node))
	}
	for _, node := range append(raw.SQLNodes, raw.SQLNodesArray...) {
		topology.SQLNodes = append(topology.SQLNodes, openGeminiMetaNodeFromJSONDataNode(node))
	}
	for name, db := range raw.Databases {
		topology.Databases = append(topology.Databases, openGeminiMetaDatabaseFromJSON(name, db))
	}
	for _, db := range raw.DatabasesArray {
		topology.Databases = append(topology.Databases, openGeminiMetaDatabaseFromJSON("", db))
	}
	for db, pts := range raw.PtView {
		topology.PtView[db] = openGeminiMetaPtInfoFromJSON(pts)
	}
	for db, pts := range raw.PtViewObject {
		topology.PtView[db] = openGeminiMetaPtInfoFromJSON(pts)
	}
	for db, groups := range raw.ReplicaGroups {
		topology.ReplicaGroups[db] = len(groups)
	}
	for db, groups := range raw.ReplicaGroupsData {
		topology.ReplicaGroups[db] = len(groups)
	}
	if !looksLikeOpenGeminiMetaTopology(topology) {
		return openGeminiMetaTopology{}, fmt.Errorf("not an openGemini meta topology json")
	}
	return topology, nil
}

func openGeminiMetaNodeFromJSONDataNode(node openGeminiMetaJSONDataNode) openGeminiMetaNode {
	base := node.openGeminiMetaJSONNode
	if node.Ni != nil {
		base = *node.Ni
	} else if node.NodeInfo != nil {
		base = *node.NodeInfo
	}
	out := openGeminiMetaNodeFromJSON(base)
	out.ConnID = node.ConnID
	out.AliveConnID = node.AliveConnID
	out.AZ = node.AZ
	return out
}

func openGeminiMetaNodeFromJSON(node openGeminiMetaJSONNode) openGeminiMetaNode {
	return openGeminiMetaNode{
		ID:              node.ID,
		Host:            node.Host,
		TCPHost:         node.TCPHost,
		RPCAddr:         node.RPCAddr,
		GossipAddr:      node.GossipAddr,
		Status:          node.Status,
		LTime:           node.LTime,
		Role:            node.Role,
		SegregateStatus: node.SegregateStatus,
	}
}

func openGeminiMetaDatabaseFromJSON(name string, db openGeminiMetaJSONDatabase) openGeminiMetaDatabase {
	if db.Name == "" {
		db.Name = name
	}
	out := openGeminiMetaDatabase{
		Name:                   db.Name,
		DefaultRetentionPolicy: db.DefaultRetentionPolicy,
		ReplicaN:               db.ReplicaN,
		MarkDeleted:            db.MarkDeleted,
	}
	for name, rp := range db.RetentionPolicies {
		out.RetentionPolicies = append(out.RetentionPolicies, openGeminiMetaRetentionPolicyFromJSON(name, rp))
	}
	for _, rp := range db.RetentionPoliciesArray {
		out.RetentionPolicies = append(out.RetentionPolicies, openGeminiMetaRetentionPolicyFromJSON("", rp))
	}
	return out
}

func openGeminiMetaRetentionPolicyFromJSON(name string, rp openGeminiMetaJSONRetentionPolicy) openGeminiMetaRetentionPolicy {
	if rp.Name == "" {
		rp.Name = name
	}
	measurementCount := countOpenGeminiMetaJSONArrayOrObject(rp.Measurements)
	return openGeminiMetaRetentionPolicy{
		Name:               rp.Name,
		Duration:           rp.Duration,
		ShardGroupDuration: rp.ShardGroupDuration,
		ReplicaN:           rp.ReplicaN,
		MarkDeleted:        rp.MarkDeleted,
		MeasurementCount:   measurementCount,
		ShardGroupCount:    len(rp.ShardGroups),
		IndexGroupCount:    len(rp.IndexGroups),
	}
}

func openGeminiMetaPtInfoFromJSON(pts []openGeminiMetaJSONPtInfo) []openGeminiMetaPtInfo {
	out := make([]openGeminiMetaPtInfo, 0, len(pts))
	for _, pt := range pts {
		ptID := pt.PtID
		if ptID == 0 {
			ptID = pt.PtID2
		}
		out = append(out, openGeminiMetaPtInfo{
			PtID:        ptID,
			OwnerNodeID: pt.Owner.NodeID,
			Status:      pt.Status,
			Version:     pt.Ver,
			RGID:        pt.RGID,
		})
	}
	return out
}

const (
	protoWireVarint  = 0
	protoWireFixed64 = 1
	protoWireBytes   = 2
	protoWireFixed32 = 5
)

func forEachProtoField(data []byte, fn func(field int, wire int, value uint64, payload []byte) error) error {
	for len(data) > 0 {
		key, n := readProtoVarint(data)
		if n <= 0 {
			return fmt.Errorf("invalid protobuf field key")
		}
		data = data[n:]
		field := int(key >> 3)
		wire := int(key & 0x07)
		if field <= 0 {
			return fmt.Errorf("invalid protobuf field number %d", field)
		}
		switch wire {
		case protoWireVarint:
			value, n := readProtoVarint(data)
			if n <= 0 {
				return fmt.Errorf("invalid protobuf varint field %d", field)
			}
			data = data[n:]
			if err := fn(field, wire, value, nil); err != nil {
				return err
			}
		case protoWireFixed64:
			if len(data) < 8 {
				return fmt.Errorf("short protobuf fixed64 field %d", field)
			}
			payload := data[:8]
			data = data[8:]
			if err := fn(field, wire, 0, payload); err != nil {
				return err
			}
		case protoWireBytes:
			size, n := readProtoVarint(data)
			if n <= 0 {
				return fmt.Errorf("invalid protobuf bytes length field %d", field)
			}
			data = data[n:]
			if size > uint64(len(data)) {
				return fmt.Errorf("protobuf bytes field %d length %d exceeds remaining %d", field, size, len(data))
			}
			payload := data[:int(size)]
			data = data[int(size):]
			if err := fn(field, wire, 0, payload); err != nil {
				return err
			}
		case protoWireFixed32:
			if len(data) < 4 {
				return fmt.Errorf("short protobuf fixed32 field %d", field)
			}
			payload := data[:4]
			data = data[4:]
			if err := fn(field, wire, 0, payload); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported protobuf wire type %d for field %d", wire, field)
		}
	}
	return nil
}

func readProtoVarint(data []byte) (uint64, int) {
	var value uint64
	for i, b := range data {
		if i == 10 {
			return 0, -1
		}
		value |= uint64(b&0x7f) << uint(7*i)
		if b < 0x80 {
			return value, i + 1
		}
	}
	return 0, -1
}

func countOpenGeminiMetaJSONArrayOrObject(data json.RawMessage) int {
	if len(data) == 0 {
		return 0
	}
	var list []json.RawMessage
	if err := json.Unmarshal(data, &list); err == nil {
		return len(list)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err == nil {
		return len(object)
	}
	return 0
}

func openGeminiMetaMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
