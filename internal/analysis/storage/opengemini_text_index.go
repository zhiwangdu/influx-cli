package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	opengeminiTextIndexType       = "opengemini-text-index"
	opengeminiTextIndexLayout     = "attached-text-index"
	opengeminiTextIndexDataSuffix = ".pos"
	opengeminiTextIndexHeadSuffix = ".bh"
	opengeminiTextIndexPartSuffix = ".ph"
	opengeminiTextIndexSkipNotice = "openGemini text index analysis is skipped"
)

type opengeminiTextIndexPaths struct {
	Base           string
	Field          string
	InputComponent string
	DataPath       string
	HeadPath       string
	PartPath       string
}

func analyzeOpenGeminiTextIndex(path string, info os.FileInfo, _ Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("opengemini-text-index format requires a .pos, .bh, or .ph file")
	}
	paths, err := openGeminiTextIndexPaths(path)
	if err != nil {
		return FileReport{}, err
	}
	extra := map[string]string{
		"type":            opengeminiTextIndexType,
		"layout":          opengeminiTextIndexLayout,
		"field":           paths.Field,
		"input_component": paths.InputComponent,
		"skipped":         "true",
		"skip_reason":     "text_index_analysis_disabled",
		"local_only":      "true",
	}
	return FileReport{
		Path:      path,
		Format:    FormatOpenGeminiText,
		SizeBytes: info.Size(),
		ModTime:   info.ModTime(),
		Extra:     extra,
		Notices:   []string{opengeminiTextIndexSkipNotice},
	}, nil
}

func openGeminiTextIndexPaths(path string) (opengeminiTextIndexPaths, error) {
	suffix, component, ok := openGeminiTextIndexSuffix(path)
	if !ok {
		return opengeminiTextIndexPaths{}, fmt.Errorf("opengemini-text-index format requires a .pos, .bh, or .ph file")
	}
	base := path[:len(path)-len(suffix)]
	field := openGeminiTextIndexField(base)
	return opengeminiTextIndexPaths{
		Base:           base,
		Field:          field,
		InputComponent: component,
		DataPath:       base + opengeminiTextIndexDataSuffix,
		HeadPath:       base + opengeminiTextIndexHeadSuffix,
		PartPath:       base + opengeminiTextIndexPartSuffix,
	}, nil
}

func openGeminiTextIndexSuffix(path string) (suffix string, component string, ok bool) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, opengeminiTextIndexDataSuffix):
		return path[len(path)-len(opengeminiTextIndexDataSuffix):], "data", true
	case strings.HasSuffix(lower, opengeminiTextIndexHeadSuffix):
		return path[len(path)-len(opengeminiTextIndexHeadSuffix):], "head", true
	case strings.HasSuffix(lower, opengeminiTextIndexPartSuffix):
		return path[len(path)-len(opengeminiTextIndexPartSuffix):], "part", true
	default:
		return "", "", false
	}
}

func openGeminiTextIndexField(base string) string {
	name := filepath.Base(base)
	if idx := strings.LastIndex(name, "."); idx >= 0 && idx+1 < len(name) {
		return name[idx+1:]
	}
	return ""
}

func isOpenGeminiTextIndexPath(path string) bool {
	_, _, ok := openGeminiTextIndexSuffix(path)
	return ok
}
