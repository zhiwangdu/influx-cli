package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestAnalyzeOpenGeminiMetaTopologyProto(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.pb")
	if err := os.WriteFile(path, testOpenGeminiMetaProto(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   8,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiMeta; got != want {
		t.Fatalf("format = %s, want %s", got, want)
	}
	if got, want := file.Extra["encoding"], "protobuf"; got != want {
		t.Fatalf("encoding = %q, want %q", got, want)
	}
	if got, want := file.Extra["term"], "4"; got != want {
		t.Fatalf("term = %q, want %q", got, want)
	}
	if got, want := file.Extra["index"], "99"; got != want {
		t.Fatalf("index = %q, want %q", got, want)
	}
	if got, want := file.Extra["cluster_id"], "12345"; got != want {
		t.Fatalf("cluster id = %q, want %q", got, want)
	}
	if got, want := file.Extra["databases"], "1"; got != want {
		t.Fatalf("databases = %q, want %q", got, want)
	}
	if got, want := file.Extra["retention_policies"], "1"; got != want {
		t.Fatalf("retention policies = %q, want %q", got, want)
	}
	if got, want := file.Extra["meta_nodes"], "1"; got != want {
		t.Fatalf("meta nodes = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_nodes"], "1"; got != want {
		t.Fatalf("data nodes = %q, want %q", got, want)
	}
	if got, want := file.Extra["sql_nodes"], "1"; got != want {
		t.Fatalf("sql nodes = %q, want %q", got, want)
	}
	if got, want := file.Extra["pts"], "2"; got != want {
		t.Fatalf("pts = %q, want %q", got, want)
	}
	if got, want := file.Extra["replica_groups"], "1"; got != want {
		t.Fatalf("replica groups = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["database"], 1; got != want {
		t.Fatalf("database blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["retention-policy"], 1; got != want {
		t.Fatalf("retention policy blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["pt"], 2; got != want {
		t.Fatalf("pt blocks = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{
		"db:metrics",
		"rp:metrics/autogen",
		"data-node:2@store:8400",
		"sql-node:3@sql:8400",
		"pt:metrics/0->node:2",
		"pt:metrics/1->node:2",
	}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
}

func TestAnalyzeOpenGeminiMetaTopologyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.ogmeta")
	data := []byte(`{
		"Term": 7,
		"Index": 44,
		"ClusterID": 77,
		"ClusterPtNum": 4,
		"PtNumPerNode": 2,
		"DataNodes": [{"Ni": {"ID": 10, "Host": "store-a", "TCPHost": "store-a:8401", "Status": 1}, "Az": "az-a"}],
		"MetaNodes": [{"ID": 1, "Host": "meta-a", "TCPHost": "meta-a:8401", "Status": 1}],
		"SqlNodes": [{"Ni": {"ID": 20, "Host": "sql-a", "TCPHost": "sql-a:8401", "Status": 1}}],
		"Databases": {
			"db0": {
				"Name": "db0",
				"DefaultRetentionPolicy": "rp0",
				"RetentionPolicies": {
					"rp0": {"Name": "rp0", "ReplicaN": 1, "Measurements": {"cpu": {}, "mem": {}}, "ShardGroups": [{}, {}]}
				}
			}
		},
		"PtView": {"db0": [{"Owner": {"NodeID": 10}, "PtId": 0, "Status": 1}]},
		"ReplicaGroups": {"db0": [{"ID": 0}]}
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiMeta,
		KeySampleLimit:   4,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["encoding"], "json"; got != want {
		t.Fatalf("encoding = %q, want %q", got, want)
	}
	if got, want := file.Extra["databases"], "1"; got != want {
		t.Fatalf("databases = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_nodes"], "1"; got != want {
		t.Fatalf("data nodes = %q, want %q", got, want)
	}
	if got, want := file.Extra["sql_nodes"], "1"; got != want {
		t.Fatalf("sql nodes = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["retention-policy"], 1; got != want {
		t.Fatalf("retention policy blocks = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].ValueCount, 2; got != want {
		t.Fatalf("rp measurement count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].ContainedChunks, 2; got != want {
		t.Fatalf("rp contained chunks = %d, want %d", got, want)
	}
}

func TestOpenGeminiMetaAutoDetectNames(t *testing.T) {
	trueNames := []string{
		"meta.pb",
		"opengemini-meta.pb",
		"opengemini-meta.json",
		"cluster.ogmeta",
	}
	for _, name := range trueNames {
		if !isOpenGeminiMetaPath(name) {
			t.Fatalf("isOpenGeminiMetaPath(%q) = false, want true", name)
		}
	}

	falseNames := []string{
		"metadata.json",
		"meta.json",
		"segment.meta",
		"primary.meta",
	}
	for _, name := range falseNames {
		if isOpenGeminiMetaPath(name) {
			t.Fatalf("isOpenGeminiMetaPath(%q) = true, want false", name)
		}
	}
}

func testOpenGeminiMetaProto() []byte {
	return testProtoMessage(
		testProtoVarintField(1, 4),
		testProtoVarintField(2, 99),
		testProtoVarintField(3, 12345),
		testProtoBytesField(5, testOpenGeminiMetaDatabaseProto()),
		testProtoBytesField(10, testOpenGeminiMetaDataNodeProto(2, "store:8400", "store:8401", "store", 1)),
		testProtoBytesField(11, testOpenGeminiMetaNodeProto(1, "meta:8400", "meta:8401", "meta", 1)),
		testProtoVarintField(14, 2),
		testProtoBytesField(15, testOpenGeminiMetaPtViewEntryProto()),
		testProtoVarintField(16, 2),
		testProtoBytesField(28, testOpenGeminiMetaReplicaGroupEntryProto()),
		testProtoBytesField(33, testOpenGeminiMetaDataNodeProto(3, "sql:8400", "sql:8401", "sql", 1)),
	)
}

func testOpenGeminiMetaDatabaseProto() []byte {
	return testProtoMessage(
		testProtoBytesField(1, []byte("metrics")),
		testProtoBytesField(2, []byte("autogen")),
		testProtoBytesField(3, testOpenGeminiMetaRetentionPolicyProto()),
		testProtoVarintField(8, 1),
	)
}

func testOpenGeminiMetaRetentionPolicyProto() []byte {
	return testProtoMessage(
		testProtoBytesField(1, []byte("autogen")),
		testProtoVarintField(2, 0),
		testProtoVarintField(3, uint64(24*60*60*1_000_000_000)),
		testProtoVarintField(4, 1),
		testProtoBytesField(5, testProtoMessage(testProtoBytesField(1, []byte("cpu")))),
		testProtoBytesField(6, testProtoMessage(testProtoVarintField(1, 100))),
		testProtoBytesField(12, testProtoMessage(testProtoVarintField(1, 200))),
	)
}

func testOpenGeminiMetaDataNodeProto(id uint64, host string, tcpHost string, role string, status uint64) []byte {
	return testProtoMessage(
		testProtoBytesField(1, testOpenGeminiMetaNodeProto(id, host, tcpHost, role, status)),
		testProtoVarintField(2, id*10),
		testProtoVarintField(3, id*10+1),
		testProtoBytesField(4, []byte("az-a")),
	)
}

func testOpenGeminiMetaNodeProto(id uint64, host string, tcpHost string, role string, status uint64) []byte {
	return testProtoMessage(
		testProtoVarintField(1, id),
		testProtoBytesField(2, []byte(host)),
		testProtoBytesField(3, []byte(tcpHost)),
		testProtoVarintField(4, status),
		testProtoBytesField(5, []byte(host+"-rpc")),
		testProtoVarintField(6, 11),
		testProtoBytesField(7, []byte(host+"-gossip")),
		testProtoBytesField(10, []byte(role)),
	)
}

func testOpenGeminiMetaPtViewEntryProto() []byte {
	return testProtoMessage(
		testProtoBytesField(1, []byte("metrics")),
		testProtoBytesField(2, testProtoMessage(
			testProtoBytesField(1, testOpenGeminiMetaPtInfoProto(0, 2)),
			testProtoBytesField(1, testOpenGeminiMetaPtInfoProto(1, 2)),
		)),
	)
}

func testOpenGeminiMetaPtInfoProto(ptID uint64, owner uint64) []byte {
	return testProtoMessage(
		testProtoBytesField(1, testProtoMessage(testProtoVarintField(1, owner))),
		testProtoVarintField(2, 1),
		testProtoVarintField(3, ptID),
		testProtoVarintField(4, 9),
		testProtoVarintField(5, 0),
	)
}

func testOpenGeminiMetaReplicaGroupEntryProto() []byte {
	return testProtoMessage(
		testProtoBytesField(1, []byte("metrics")),
		testProtoBytesField(2, testProtoMessage(
			testProtoBytesField(1, testProtoMessage(testProtoVarintField(1, 0))),
		)),
	)
}

func testProtoMessage(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

func testProtoVarintField(field int, value uint64) []byte {
	var out []byte
	out = binary.AppendUvarint(out, uint64(field<<3|protoWireVarint))
	out = binary.AppendUvarint(out, value)
	return out
}

func testProtoBytesField(field int, value []byte) []byte {
	var out []byte
	out = binary.AppendUvarint(out, uint64(field<<3|protoWireBytes))
	out = binary.AppendUvarint(out, uint64(len(value)))
	out = append(out, value...)
	return out
}
