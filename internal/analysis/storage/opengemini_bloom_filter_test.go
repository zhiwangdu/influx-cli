package storage

import (
	"bufio"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeOpenGeminiBloomFilterAttachedLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000001-0001-00000001.content.bf")
	if err := writeTestOpenGeminiBloomFilterLineFile(path, 2, false, 0); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiBloom; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.KeyCount, 1; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"field:content"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["bloom-filter-line-block"], 2; got != want {
		t.Fatalf("line blocks = %d, want %d", got, want)
	}
	if file.SecondaryIndex == nil {
		t.Fatal("secondary index summary is nil")
	}
	if got, want := file.SecondaryIndex.Layout, opengeminiBloomFilterLayoutLine; got != want {
		t.Fatalf("layout = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.Field, "content"; got != want {
		t.Fatalf("field = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.BlockCount, int64(2); got != want {
		t.Fatalf("secondary block count = %d, want %d", got, want)
	}
	if got := file.SecondaryIndex.CRCMismatches; got != 0 {
		t.Fatalf("crc mismatches = %d, want 0", got)
	}
	if got, want := len(file.Blocks), 2; got != want {
		t.Fatalf("blocks = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].Offset, opengeminiBloomFilterBlockSize; got != want {
		t.Fatalf("second block offset = %d, want %d", got, want)
	}
	if got, want := file.Extra["local_only"], "true"; got != want {
		t.Fatalf("local_only = %q, want %q", got, want)
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeOpenGeminiBloomFilterCorruptLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000001-0001-00000001.bloomfilter_fullText.bf")
	if err := writeTestOpenGeminiBloomFilterLineFile(path, 1, true, 3); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiBloom,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.KeySamples, []string{"field:fullText (full-text)"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if got, want := file.SecondaryIndex.CRCMismatches, 1; got != want {
		t.Fatalf("crc mismatches = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.TrailingBytes, int64(3); got != want {
		t.Fatalf("trailing bytes = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["bloom-filter-crc-mismatch"], 1; got != want {
		t.Fatalf("crc mismatch blocks = %d, want %d", got, want)
	}
	if !containsOpenGeminiBloomNotice(file.Notices, "CRC mismatch") {
		t.Fatalf("notices = %v, want CRC mismatch notice", file.Notices)
	}
	if !containsOpenGeminiBloomNotice(file.Notices, "3 trailing byte") {
		t.Fatalf("notices = %v, want trailing bytes notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiBloomFilterDetachedVerticalEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bloomfilter_content.idx")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiBloom; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.Layout, opengeminiBloomFilterLayoutVertical; got != want {
		t.Fatalf("layout = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.Field, "content"; got != want {
		t.Fatalf("field = %q, want %q", got, want)
	}
	if got := file.SecondaryIndex.BlockCount; got != 0 {
		t.Fatalf("block count = %d, want 0", got)
	}
	if !containsOpenGeminiBloomNotice(file.Notices, "empty") {
		t.Fatalf("notices = %v, want empty file notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiBloomFilterDetachedVertical(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bloomfilter_content.idx")
	if err := writeTestOpenGeminiBloomFilterVerticalFile(path, true); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   1,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiBloom; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.BlockCount, int(opengeminiBloomFilterVerticalGroupSize); got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["bloom-filter-vertical-group"], 1; got != want {
		t.Fatalf("vertical groups = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["bloom-filter-vertical-piece"], int(opengeminiBloomFilterVerticalPieces); got != want {
		t.Fatalf("vertical pieces = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.GroupCount, int64(1); got != want {
		t.Fatalf("group count = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.PieceCount, opengeminiBloomFilterVerticalPieces; got != want {
		t.Fatalf("piece count = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.CRCMismatches, 1; got != want {
		t.Fatalf("crc mismatches = %d, want %d", got, want)
	}
	if got, want := len(file.Blocks), 1; got != want {
		t.Fatalf("block samples = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].SizeBytes, clampUint32(opengeminiBloomFilterVerticalDiskSize); got != want {
		t.Fatalf("sample size = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].SegmentCount, int(opengeminiBloomFilterVerticalGroupSize); got != want {
		t.Fatalf("sample segment count = %d, want %d", got, want)
	}
	if !containsOpenGeminiBloomNotice(file.Notices, "CRC mismatch") {
		t.Fatalf("notices = %v, want CRC mismatch notice", file.Notices)
	}
}

func TestOpenGeminiBloomFilterVerticalSamplesUseGroupCount(t *testing.T) {
	blocks := openGeminiBloomFilterBlockSamples(opengeminiBloomFilterAnalysis{
		PathInfo: opengeminiBloomFilterPathInfo{
			Field:  "content",
			Layout: opengeminiBloomFilterLayoutVertical,
		},
		BlockCount: 128,
		GroupCount: 1,
	}, 3)
	if got, want := len(blocks), 1; got != want {
		t.Fatalf("blocks = %d, want %d", got, want)
	}
	if got, want := blocks[0].Offset, int64(0); got != want {
		t.Fatalf("offset = %d, want %d", got, want)
	}
	if got, want := blocks[0].SegmentCount, int(opengeminiBloomFilterVerticalGroupSize); got != want {
		t.Fatalf("segment count = %d, want %d", got, want)
	}
}

func TestAnalyzeOpenGeminiBloomFilterOlderSizeNotice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "00000001-0001-00000001.content.bf")
	if err := os.WriteFile(path, make([]byte, 32*1024+64+4), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiBloom})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got := file.SecondaryIndex.ValidBytes; got != 0 {
		t.Fatalf("valid bytes = %d, want 0", got)
	}
	if !containsOpenGeminiBloomNotice(file.Notices, "older logstore filter size") {
		t.Fatalf("notices = %v, want older size notice", file.Notices)
	}
}

func writeTestOpenGeminiBloomFilterLineFile(path string, blocks int, corruptCRC bool, trailingBytes int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	payload := make([]byte, opengeminiBloomFilterPayloadSize)
	for i := 0; i < blocks; i++ {
		payload[0] = byte(i + 1)
		if _, err := f.Write(payload); err != nil {
			return err
		}
		crc := crc32.Checksum(payload, opengeminiBloomFilterCRCTable)
		if corruptCRC && i == 0 {
			crc++
		}
		var crcBuf [4]byte
		binary.LittleEndian.PutUint32(crcBuf[:], crc)
		if _, err := f.Write(crcBuf[:]); err != nil {
			return err
		}
		payload[0] = 0
	}
	if trailingBytes > 0 {
		_, err = f.Write(make([]byte, trailingBytes))
	}
	return err
}

func writeTestOpenGeminiBloomFilterVerticalFile(path string, corruptCRC bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1024*1024)
	payload := make([]byte, opengeminiBloomFilterVerticalPieceMem)
	for i := int64(0); i < opengeminiBloomFilterVerticalPieces; i++ {
		binary.LittleEndian.PutUint64(payload[:8], uint64(i+1))
		if _, err := w.Write(payload); err != nil {
			return err
		}
		crc := crc32.Checksum(payload, opengeminiBloomFilterCRCTable)
		if corruptCRC && i == 0 {
			crc++
		}
		var crcBuf [4]byte
		binary.LittleEndian.PutUint32(crcBuf[:], crc)
		if _, err := w.Write(crcBuf[:]); err != nil {
			return err
		}
	}
	return w.Flush()
}

func containsOpenGeminiBloomNotice(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
