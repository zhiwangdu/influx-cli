package influxdb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
	"github.com/zhiwangdu/influx-cli/internal/query"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

type Config struct {
	Name            string
	URL             string
	Username        string
	Password        string
	Token           string
	Database        string
	RetentionPolicy string
	Precision       string
	HTTPClient      *http.Client
}

type Adapter struct {
	name             string
	baseURL          *url.URL
	username         string
	password         string
	token            string
	defaultDatabase  string
	defaultRP        string
	defaultPrecision string
	client           *http.Client
}

func New(config Config) (*Adapter, error) {
	rawURL := strings.TrimSpace(config.URL)
	if rawURL == "" {
		rawURL = "http://127.0.0.1:8086"
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse adapter URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("adapter URL must include scheme and host")
	}

	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = "influxdb"
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	return &Adapter{
		name:             name,
		baseURL:          parsed,
		username:         config.Username,
		password:         config.Password,
		token:            config.Token,
		defaultDatabase:  config.Database,
		defaultRP:        config.RetentionPolicy,
		defaultPrecision: config.Precision,
		client:           client,
	}, nil
}

func (a *Adapter) Name() string {
	return a.name
}

func (a *Adapter) Ping(ctx context.Context) error {
	pingURL := a.endpoint("/ping")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pingURL.String(), nil)
	if err != nil {
		return fmt.Errorf("build ping request: %w", err)
	}
	a.addAuth(request)

	response, err := a.client.Do(request)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", a.baseURL.Host, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	return httpStatusError(response.StatusCode, body)
}

func (a *Adapter) Query(ctx context.Context, q query.Query) (result.Result, error) {
	body, precision, err := a.queryRaw(ctx, q)
	if err != nil {
		return result.Result{}, err
	}
	normalized, err := NormalizeResponse(body, precision, a.name)
	if err != nil {
		return result.Result{}, err
	}
	return normalized, nil
}

func (a *Adapter) queryRaw(ctx context.Context, q query.Query) ([]byte, string, error) {
	form := url.Values{}
	form.Set("q", q.Raw)
	if q.Database != "" {
		form.Set("db", q.Database)
	} else if a.defaultDatabase != "" {
		form.Set("db", a.defaultDatabase)
	}
	if q.RP != "" {
		form.Set("rp", q.RP)
	} else if a.defaultRP != "" {
		form.Set("rp", a.defaultRP)
	}
	precision := q.Precision
	if precision == "" {
		precision = a.defaultPrecision
	}
	if shouldSendEpoch(precision) {
		form.Set("epoch", strings.ToLower(precision))
	}

	queryURL := a.endpoint("/query")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return nil, "", fmt.Errorf("build query request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	a.addAuth(request)

	response, err := a.client.Do(request)
	if err != nil {
		return nil, "", fmt.Errorf("query %s: %w", a.baseURL.Host, err)
	}
	defer response.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(response.Body, 10*1024*1024))
	if readErr != nil {
		return nil, "", fmt.Errorf("read query response: %w", readErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, "", httpStatusError(response.StatusCode, body)
	}
	return body, precision, nil
}

func (a *Adapter) rawQueryResponse(ctx context.Context, q query.Query) (queryResponse, error) {
	body, _, err := a.queryRaw(ctx, q)
	if err != nil {
		return queryResponse{}, err
	}
	return decodeQueryResponse(body)
}

func (a *Adapter) ShowDatabases(ctx context.Context) ([]string, error) {
	res, err := a.Query(ctx, query.New("SHOW DATABASES", "", "", a.defaultPrecision))
	if err != nil {
		return nil, err
	}
	values := valuesFromColumn(res.Table, "name")
	sort.Strings(values)
	return values, nil
}

func (a *Adapter) ShowRetentionPolicies(ctx context.Context, db string) ([]adapter.RetentionPolicy, error) {
	res, err := a.Query(ctx, query.New("SHOW RETENTION POLICIES ON "+quoteIdentifier(db), db, "", a.defaultPrecision))
	if err != nil {
		return nil, err
	}
	policies := retentionPoliciesFromTable(res.Table)
	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Name < policies[j].Name
	})
	return policies, nil
}

func (a *Adapter) ShowMeasurements(ctx context.Context, db, rp string) ([]string, error) {
	res, err := a.Query(ctx, query.New("SHOW MEASUREMENTS", db, rp, a.defaultPrecision))
	if err != nil {
		return nil, err
	}
	values := valuesFromColumn(res.Table, "name")
	sort.Strings(values)
	return values, nil
}

func (a *Adapter) GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error) {
	database := scope.Database
	if database == "" {
		database = a.defaultDatabase
	}
	retentionPolicy := scope.RetentionPolicy
	if retentionPolicy == "" {
		retentionPolicy = a.defaultRP
	}

	measurement := strings.TrimSpace(scope.Measurement)
	fieldQuery := "SHOW FIELD KEYS"
	tagQuery := "SHOW TAG KEYS"
	if measurement != "" {
		measurementExpr := quoteIdentifier(measurement)
		fieldQuery += " FROM " + measurementExpr
		tagQuery += " FROM " + measurementExpr
	}

	fields, err := a.rawQueryResponse(ctx, query.New(fieldQuery, database, retentionPolicy, a.defaultPrecision))
	if err != nil {
		return schema.Snapshot{}, err
	}
	tags, err := a.rawQueryResponse(ctx, query.New(tagQuery, database, retentionPolicy, a.defaultPrecision))
	if err != nil {
		return schema.Snapshot{}, err
	}

	snapshot := schema.Snapshot{
		Database:        database,
		RetentionPolicy: retentionPolicy,
	}
	index := map[string]int{}
	appendSchemaFields(&snapshot, index, fields, measurement)
	appendSchemaTags(&snapshot, index, tags, measurement)
	sortSchemaSnapshot(&snapshot)
	return snapshot, nil
}

func appendSchemaFields(snapshot *schema.Snapshot, index map[string]int, response queryResponse, fallbackMeasurement string) {
	for _, statement := range response.Results {
		for _, series := range statement.Series {
			measurement := schemaMeasurementName(series.Name, fallbackMeasurement)
			if measurement == "" {
				continue
			}
			target := schemaMeasurement(snapshot, index, measurement)
			for _, row := range series.Values {
				name := firstNonEmptyString(stringAt(row, series.Columns, "fieldKey"), stringAt(row, series.Columns, "field_key"))
				if name == "" {
					continue
				}
				target.Fields = append(target.Fields, schema.Field{
					Name: name,
					Type: firstNonEmptyString(stringAt(row, series.Columns, "fieldType"), stringAt(row, series.Columns, "field_type")),
				})
			}
		}
	}
}

func appendSchemaTags(snapshot *schema.Snapshot, index map[string]int, response queryResponse, fallbackMeasurement string) {
	for _, statement := range response.Results {
		for _, series := range statement.Series {
			measurement := schemaMeasurementName(series.Name, fallbackMeasurement)
			if measurement == "" {
				continue
			}
			target := schemaMeasurement(snapshot, index, measurement)
			for _, row := range series.Values {
				name := firstNonEmptyString(stringAt(row, series.Columns, "tagKey"), stringAt(row, series.Columns, "tag_key"))
				if name == "" {
					continue
				}
				target.Tags = append(target.Tags, schema.Tag{Name: name})
			}
		}
	}
}

func schemaMeasurement(snapshot *schema.Snapshot, index map[string]int, name string) *schema.Measurement {
	if i, ok := index[name]; ok {
		return &snapshot.Measurements[i]
	}
	snapshot.Measurements = append(snapshot.Measurements, schema.Measurement{Name: name})
	index[name] = len(snapshot.Measurements) - 1
	return &snapshot.Measurements[len(snapshot.Measurements)-1]
}

func schemaMeasurementName(name, fallback string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = strings.TrimSpace(fallback)
	}
	return strings.Trim(name, ` "'`)
}

func sortSchemaSnapshot(snapshot *schema.Snapshot) {
	sort.Slice(snapshot.Measurements, func(i, j int) bool {
		return snapshot.Measurements[i].Name < snapshot.Measurements[j].Name
	})
	for i := range snapshot.Measurements {
		measurement := &snapshot.Measurements[i]
		sort.Slice(measurement.Fields, func(i, j int) bool {
			return measurement.Fields[i].Name < measurement.Fields[j].Name
		})
		sort.Slice(measurement.Tags, func(i, j int) bool {
			return measurement.Tags[i].Name < measurement.Tags[j].Name
		})
	}
}

func (a *Adapter) endpoint(path string) *url.URL {
	next := *a.baseURL
	basePath := strings.TrimRight(next.Path, "/")
	next.Path = basePath + path
	return &next
}

func (a *Adapter) addAuth(request *http.Request) {
	if a.username != "" || a.password != "" {
		request.SetBasicAuth(a.username, a.password)
	}
	if a.token != "" {
		request.Header.Set("Authorization", "Token "+a.token)
	}
}

func shouldSendEpoch(precision string) bool {
	switch strings.ToLower(strings.TrimSpace(precision)) {
	case "", "rfc3339", "rfc3339nano":
		return false
	case "h", "m", "s", "ms", "u", "us", "ns":
		return true
	default:
		return false
	}
}

func httpStatusError(status int, body []byte) error {
	message := strings.TrimSpace(string(body))
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		if message == "" {
			return fmt.Errorf("authentication failed with HTTP %d", status)
		}
		return fmt.Errorf("authentication failed with HTTP %d: %s", status, message)
	}
	if message == "" {
		return fmt.Errorf("InfluxDB HTTP request failed with status %d", status)
	}
	return fmt.Errorf("InfluxDB HTTP request failed with status %d: %s", status, message)
}

func valuesFromColumn(table *result.Table, column string) []string {
	if table == nil {
		return nil
	}
	index := -1
	for i, name := range table.Columns {
		if strings.EqualFold(name, column) {
			index = i
			break
		}
	}
	if index < 0 {
		return nil
	}
	var values []string
	for _, row := range table.Rows {
		if index >= len(row) {
			continue
		}
		value := fmt.Sprint(row[index])
		if value != "" && value != "<nil>" {
			values = append(values, value)
		}
	}
	return values
}

func quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, `"`)
		part = strings.ReplaceAll(part, `\`, `\\`)
		part = strings.ReplaceAll(part, `"`, `\"`)
		quoted = append(quoted, `"`+part+`"`)
	}
	return strings.Join(quoted, ".")
}

func tableRows(table *result.Table) [][]any {
	if table == nil {
		return nil
	}
	return table.Rows
}

func stringAt(row []any, columns []string, column string) string {
	for i, name := range columns {
		if !strings.EqualFold(name, column) || i >= len(row) || row[i] == nil {
			continue
		}
		return fmt.Sprint(row[i])
	}
	return ""
}

func boolAt(row []any, columns []string, column string) bool {
	for i, name := range columns {
		if !strings.EqualFold(name, column) || i >= len(row) || row[i] == nil {
			continue
		}
		switch value := row[i].(type) {
		case bool:
			return value
		case string:
			return strings.EqualFold(value, "true")
		default:
			return strings.EqualFold(fmt.Sprint(value), "true")
		}
	}
	return false
}

func retentionPoliciesFromTable(table *result.Table) []adapter.RetentionPolicy {
	var policies []adapter.RetentionPolicy
	for _, row := range tableRows(table) {
		name := stringAt(row, table.Columns, "name")
		if name == "" {
			continue
		}
		policies = append(policies, adapter.RetentionPolicy{
			Name:               name,
			Duration:           stringAt(row, table.Columns, "duration"),
			ShardGroupDuration: firstNonEmptyString(stringAt(row, table.Columns, "shardGroupDuration"), stringAt(row, table.Columns, "shard_group_duration")),
			ReplicaN:           firstNonEmptyString(stringAt(row, table.Columns, "replicaN"), stringAt(row, table.Columns, "replica_n")),
			Default:            boolAt(row, table.Columns, "default"),
		})
	}
	return policies
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
