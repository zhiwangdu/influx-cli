package adapter

import "context"

type WriteRequest struct {
	Database        string
	RetentionPolicy string
	Precision       string
	Body            []byte
}

type LineProtocolWriter interface {
	WriteLineProtocol(ctx context.Context, request WriteRequest) error
}
