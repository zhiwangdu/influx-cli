package storage

import (
	"fmt"
	"sort"
	"strings"
)

func (r Report) Markdown() string {
	var b strings.Builder
	b.WriteString("# Storage Analyzer Report\n\n")

	fileCount := r.Summary.FileCount
	if fileCount == 0 {
		fileCount = len(r.Files)
	}
	b.WriteString("## Summary\n\n")
	b.WriteString("| metric | value |\n")
	b.WriteString("| --- | --- |\n")
	appendMarkdownMetric(&b, "files", fileCount)
	appendMarkdownMetric(&b, "total_size_bytes", r.Summary.TotalSizeBytes)
	appendMarkdownMetric(&b, "keys_or_series", r.Summary.KeyCount)
	appendMarkdownMetric(&b, "blocks", r.Summary.BlockCount)
	appendMarkdownMetric(&b, "query_overlap_files", r.Summary.QueryOverlapFiles)
	appendMarkdownMetric(&b, "query_overlap_blocks", r.Summary.QueryOverlapBlocks)
	appendMarkdownMetric(&b, "tombstone_files", r.Summary.TombstoneFiles)
	appendMarkdownMetric(&b, "tombstone_size_bytes", r.Summary.TombstoneSizeBytes)
	appendMarkdownMetric(&b, "tombstone_ranges", r.Summary.TombstoneRanges)
	appendMarkdownMetric(&b, "tombstone_query_overlap_ranges", r.Summary.TombstoneQueryOverlapRanges)
	appendMarkdownMetric(&b, "tombstone_affected_blocks", r.Summary.TombstoneAffectedBlocks)
	appendMarkdownMetric(&b, "notices", len(r.Notices))
	b.WriteByte('\n')

	appendMarkdownCountTable(&b, "Block Types", r.Summary.BlocksByType)
	appendMarkdownDecodePath(&b, "File-Set Decode Path", r.DecodePath)
	appendMarkdownFiles(&b, r.Files)

	out := strings.TrimRight(b.String(), "\n")
	return out + "\n"
}

func appendMarkdownMetric(b *strings.Builder, metric string, value any) {
	fmt.Fprintf(b, "| %s | %v |\n", markdownCell(metric), value)
}

func appendMarkdownCountTable(b *strings.Builder, title string, counts map[string]int) {
	if len(counts) == 0 {
		return
	}
	keys := make([]string, 0, len(counts))
	for key, count := range counts {
		if count <= 0 {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return
	}
	sort.Strings(keys)
	b.WriteString("## ")
	b.WriteString(title)
	b.WriteString("\n\n")
	b.WriteString("| type | count |\n")
	b.WriteString("| --- | --- |\n")
	for _, key := range keys {
		fmt.Fprintf(b, "| %s | %d |\n", markdownCell(key), counts[key])
	}
	b.WriteByte('\n')
}

func appendMarkdownDecodePath(b *strings.Builder, title string, summary *DecodePathSummary) {
	if summary == nil {
		return
	}
	b.WriteString("## ")
	b.WriteString(title)
	b.WriteString("\n\n")
	if summary.Mode != "" {
		b.WriteString("- mode: `")
		b.WriteString(markdownInline(summary.Mode))
		b.WriteString("`\n")
	}
	if text := markdownDecodePathSummaryText(summary); text != "" {
		b.WriteString("- summary: ")
		b.WriteString(markdownText(text))
		b.WriteByte('\n')
	}
	recommendations := decodePathRecommendations(summary)
	if len(recommendations) > 0 {
		b.WriteString("- recommendations:\n")
		for _, recommendation := range recommendations {
			b.WriteString("  - ")
			b.WriteString(markdownText(recommendation))
			b.WriteByte('\n')
		}
	}
	b.WriteByte('\n')
}

func markdownDecodePathSummaryText(summary *DecodePathSummary) string {
	text := decodePathText(summary)
	if summary == nil || summary.Mode == "" {
		return text
	}
	if text == summary.Mode {
		return ""
	}
	return strings.TrimPrefix(text, summary.Mode+", ")
}

func appendMarkdownFiles(b *strings.Builder, files []FileReport) {
	if len(files) == 0 {
		return
	}
	b.WriteString("## Files\n\n")
	b.WriteString("| file | format | size_bytes | time_min | time_max | keys_series | blocks | query_blocks | tombstone | details | decode_path |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for i, file := range files {
		fmt.Fprintf(
			b,
			"| file-%d | %s | %d | %s | %s | %d | %d | %d | %s | %s | %s |\n",
			i+1,
			markdownCell(string(file.Format)),
			file.SizeBytes,
			markdownCell(FormatUnixNano(file.MinTime)),
			markdownCell(FormatUnixNano(file.MaxTime)),
			file.KeyCount,
			file.BlockCount,
			file.QueryOverlapBlocks,
			markdownCell(tombstoneText(file.Tombstones)),
			markdownCell(fileDetailsText(file)),
			markdownCell(decodePathText(file.DecodePath)),
		)
	}
	b.WriteByte('\n')
}

func markdownCell(value string) string {
	return strings.ReplaceAll(markdownText(value), "|", `\|`)
}

func markdownText(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}

func markdownInline(value string) string {
	return strings.ReplaceAll(markdownText(value), "`", "'")
}
