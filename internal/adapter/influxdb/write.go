package influxdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
)

func (a *Adapter) WriteLineProtocol(ctx context.Context, request adapter.WriteRequest) error {
	body := bytes.TrimSpace(request.Body)
	if len(body) == 0 {
		return nil
	}

	database := strings.TrimSpace(firstNonEmptyString(request.Database, a.defaultDatabase))
	if database == "" {
		return errors.New("database is required for line protocol writes")
	}

	writeURL := a.endpoint("/write")
	query := writeURL.Query()
	query.Set("db", database)
	if retentionPolicy := strings.TrimSpace(firstNonEmptyString(request.RetentionPolicy, a.defaultRP)); retentionPolicy != "" {
		query.Set("rp", retentionPolicy)
	}
	if precision := writePrecision(request.Precision); precision != "" {
		query.Set("precision", precision)
	}
	writeURL.RawQuery = query.Encode()

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, writeURL.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "text/plain")
	a.addAuth(httpRequest)

	response, err := a.client.Do(httpRequest)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	if readErr != nil {
		return readErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return httpStatusError(response.StatusCode, responseBody)
	}
	return nil
}

func writePrecision(precision string) string {
	switch normalized := strings.ToLower(strings.TrimSpace(precision)); normalized {
	case "", "rfc3339", "rfc3339nano":
		return ""
	case "n", "ns":
		return "n"
	case "u", "us":
		return "u"
	case "ms", "s", "m", "h":
		return normalized
	default:
		return normalized
	}
}
