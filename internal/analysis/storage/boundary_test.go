package storage

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestStorageAnalyzerProductionImportsStayLocalOnly(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate boundary test file")
	}
	dir := filepath.Dir(currentFile)
	fileset := token.NewFileSet()

	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fileset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if !storageAnalyzerAllowedProductionImport(importPath) {
				t.Errorf("%s imports %q; storage analyzer production imports must stay local-only", filepath.Base(path), importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStorageAnalyzerProductionImportBoundaryClassifiesRuntimeImports(t *testing.T) {
	for _, importPath := range []string{
		"encoding/binary",
		"github.com/golang/snappy",
		"github.com/klauspost/compress/zstd",
		"github.com/zhiwangdu/influx-cli/internal/result",
	} {
		if !storageAnalyzerAllowedProductionImport(importPath) {
			t.Fatalf("import %q should be allowed", importPath)
		}
	}

	for _, importPath := range []string{
		"net",
		"net/http",
		"os/exec",
		"database/sql",
		"google.golang.org/grpc",
		"github.com/influxdata/influxdb/client/v2",
		"github.com/openGemini/openGemini/engine/immutable",
		"github.com/openGemini/openGemini/lib/fileops",
		"github.com/openGemini/openGemini/engine/executor",
		"github.com/openGemini/openGemini/lib/util/lifted/influx/query",
		"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs",
	} {
		if storageAnalyzerAllowedProductionImport(importPath) {
			t.Fatalf("import %q should be rejected", importPath)
		}
	}
}

func storageAnalyzerAllowedProductionImport(importPath string) bool {
	if storageAnalyzerExplicitlyForbiddenProductionImport(importPath) {
		return false
	}
	if storageAnalyzerStandardLibraryImport(importPath) {
		return true
	}
	switch importPath {
	case "github.com/golang/snappy",
		"github.com/klauspost/compress/snappy",
		"github.com/klauspost/compress/zstd",
		"github.com/pierrec/lz4/v4",
		"github.com/zhiwangdu/influx-cli/internal/result":
		return true
	}
	return false
}

func storageAnalyzerExplicitlyForbiddenProductionImport(importPath string) bool {
	switch {
	case importPath == "net" || strings.HasPrefix(importPath, "net/"):
		return true
	case importPath == "os/exec":
		return true
	case importPath == "database/sql" || strings.HasPrefix(importPath, "database/"):
		return true
	case strings.HasPrefix(importPath, "google.golang.org/grpc"):
		return true
	case storageAnalyzerImportPathHasSegment(importPath, "client"):
		return true
	case storageAnalyzerImportPathHasSegment(importPath, "http"):
		return true
	case storageAnalyzerImportPathHasSegment(importPath, "engine"):
		return true
	case storageAnalyzerImportPathHasSegment(importPath, "fileops"):
		return true
	case storageAnalyzerImportPathHasSegment(importPath, "obs"):
		return true
	case storageAnalyzerImportPathHasSegment(importPath, "query"):
		return true
	}
	return false
}

func storageAnalyzerStandardLibraryImport(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}

func storageAnalyzerImportPathHasSegment(importPath string, segment string) bool {
	for _, part := range strings.Split(importPath, "/") {
		if part == segment {
			return true
		}
	}
	return false
}
