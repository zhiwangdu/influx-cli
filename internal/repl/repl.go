package repl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/render"
	"github.com/zhiwangdu/influx-cli/internal/result"
)

type Options struct {
	Render  render.Options
	History *history.Store
}

var errInputInterrupted = errors.New("input interrupted")

const lineHistoryLimit = history.DefaultMaxEntries

type lineReader interface {
	ReadLine(prompt string) (string, error)
	Close() error
}

type historyLineReader interface {
	SaveHistory(line string) error
}

func Run(ctx context.Context, executor *app.Executor, in io.Reader, out io.Writer, options Options) error {
	reader := lineReader(newScannerLineReader(in, out))
	if terminalReader, ok, err := newTerminalLineReader(ctx, executor, in, out); err != nil {
		return err
	} else if ok {
		reader = terminalReader
	}
	return runWithReader(ctx, executor, reader, out, options)
}

func runWithReader(ctx context.Context, executor *app.Executor, reader lineReader, out io.Writer, options Options) error {
	defer reader.Close()

	if err := loadLineHistory(reader, options.History); err != nil {
		fmt.Fprintln(out, "warning:", err)
	}
	fmt.Fprintln(out, "Enter run | \\ continue | Tab complete | Up/Down history | Ctrl+C cancel | :q quit | :help commands")
	var buffer statementBuffer
	for {
		if ctx.Err() != nil {
			return nil
		}
		prompt := executor.Session.Prompt()
		if buffer.Active() {
			prompt = "  -> "
		}
		line, err := reader.ReadLine(prompt)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, errInputInterrupted) {
				buffer.Reset()
				continue
			}
			return err
		}
		if buffer.Active() {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine == ":cancel" || trimmedLine == ":clear" {
				buffer.Reset()
				fmt.Fprintln(out, "query cleared")
				fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
				continue
			}
			if strings.HasPrefix(trimmedLine, ":") {
				fmt.Fprintln(out, "error: finish query with ; or cancel it first")
				fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
				continue
			}
		}
		statement, ready := buffer.Add(line)
		if !ready {
			continue
		}
		statement = normalizeStatement(statement)
		if statement == "" {
			continue
		}
		if statement == ":q" || strings.EqualFold(statement, "q") {
			return nil
		}
		if statement == ":cancel" || statement == ":clear" {
			fmt.Fprintln(out, "no pending query")
			fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
			continue
		}
		// Render format is REPL-local UI state, so it is handled before app
		// meta commands that mutate database/session context.
		if handled, err := handleFormatCommand(statement, &options, out); handled {
			if err != nil {
				fmt.Fprintln(out, "error:", err)
			}
			fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
			continue
		}
		if handled, err := handleHistoryCommand(statement, &options, out); handled {
			if err != nil {
				fmt.Fprintln(out, "error:", err)
			}
			fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
			continue
		}

		res, err := executor.Execute(ctx, statement)
		if err != nil {
			if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return nil
			}
			fmt.Fprintln(out, "error:", err)
			fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
			continue
		}
		output, _, err := render.Render(res, options.Render)
		if err != nil {
			fmt.Fprintln(out, "error:", err)
			fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
			continue
		}
		if strings.TrimSpace(output) != "" {
			fmt.Fprintln(out, output)
		}
		if shouldPersistHistory(statement) {
			if err := persistHistory(executor, options.History, statement); err != nil {
				fmt.Fprintln(out, "warning:", err)
			}
			if err := saveLineHistory(reader, statement); err != nil {
				fmt.Fprintln(out, "warning:", err)
			}
		}
		fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
	}
}

type scannerLineReader struct {
	scanner *bufio.Scanner
	out     io.Writer
}

func newScannerLineReader(in io.Reader, out io.Writer) *scannerLineReader {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	return &scannerLineReader{scanner: scanner, out: out}
}

func (r *scannerLineReader) ReadLine(prompt string) (string, error) {
	fmt.Fprint(r.out, prompt)
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}
	return r.scanner.Text(), nil
}

func (r *scannerLineReader) Close() error {
	return nil
}

func loadLineHistory(reader lineReader, store *history.Store) error {
	historyReader, ok := reader.(historyLineReader)
	if !ok || store == nil {
		return nil
	}

	entries, err := store.Search("", lineHistoryLimit)
	if err != nil {
		return fmt.Errorf("load query history: %w", err)
	}
	for i := len(entries) - 1; i >= 0; i-- {
		line := lineHistoryReplayText(entries[i].Query)
		if line == "" {
			continue
		}
		if err := historyReader.SaveHistory(line); err != nil {
			return fmt.Errorf("load query history: %w", err)
		}
	}
	return nil
}

func saveLineHistory(reader lineReader, line string) error {
	historyReader, ok := reader.(historyLineReader)
	if !ok {
		return nil
	}
	line = lineHistoryReplayText(line)
	if line == "" {
		return nil
	}
	if err := historyReader.SaveHistory(line); err != nil {
		return fmt.Errorf("save query history: %w", err)
	}
	return nil
}

func lineHistoryReplayText(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}
	var buffer statementBuffer
	if _, ready := buffer.Add(line); ready {
		return line
	}
	if strings.HasSuffix(line, ";") {
		return line
	}
	return line + ";"
}

func handleFormatCommand(line string, options *Options, out io.Writer) (bool, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false, nil
	}
	command := strings.ToLower(fields[0])
	if command != ":format" && command != ":fmt" {
		return false, nil
	}
	if len(fields) > 2 {
		return true, fmt.Errorf("usage: :format [auto|table|sparkline|json]")
	}
	if len(fields) == 1 {
		format, err := render.NormalizeFormat(options.Render.Format)
		if err != nil {
			return true, err
		}
		fmt.Fprintf(out, "format: %s\n", format)
		return true, nil
	}

	format, err := render.NormalizeFormat(fields[1])
	if err != nil {
		return true, err
	}
	options.Render.Format = format
	fmt.Fprintf(out, "format: %s\n", format)
	return true, nil
}

func handleHistoryCommand(line string, options *Options, out io.Writer) (bool, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false, nil
	}
	command := strings.ToLower(fields[0])
	if command != ":history" && command != ":hist" {
		return false, nil
	}
	if options.History == nil {
		return true, fmt.Errorf("history is not enabled")
	}

	limit, filter, err := parseHistoryArgs(fields[1:])
	if err != nil {
		return true, err
	}
	entries, err := options.History.Search(filter, limit)
	if err != nil {
		return true, err
	}
	output, _, err := render.Render(historyResult(entries), options.Render)
	if err != nil {
		return true, err
	}
	if strings.TrimSpace(output) != "" {
		fmt.Fprintln(out, output)
	}
	return true, nil
}

func parseHistoryArgs(args []string) (int, string, error) {
	if len(args) == 0 {
		return history.DefaultLimit, "", nil
	}
	limit := history.DefaultLimit
	filterStart := 0
	if parsed, err := strconv.Atoi(args[0]); err == nil {
		if parsed <= 0 {
			return 0, "", fmt.Errorf("usage: :history [limit] [filter]")
		}
		if parsed > history.DefaultMaxEntries {
			parsed = history.DefaultMaxEntries
		}
		limit = parsed
		filterStart = 1
	}
	return limit, strings.Join(args[filterStart:], " "), nil
}

func historyResult(entries []history.Entry) result.Result {
	table := result.NewTable([]string{"id", "time", "db", "rp", "dialect", "query"})
	for _, entry := range entries {
		table.AddRow(
			entry.ID,
			entry.Time.Format("2006-01-02T15:04:05Z07:00"),
			entry.Database,
			entry.RetentionPolicy,
			entry.Dialect,
			entry.Query,
		)
	}
	return result.FromTable(table)
}

func shouldPersistHistory(line string) bool {
	return !strings.HasPrefix(strings.TrimSpace(line), ":")
}

func persistHistory(executor *app.Executor, store *history.Store, line string) error {
	if store == nil {
		return nil
	}
	return store.Append(history.Entry{
		Database:        executor.Session.Database,
		RetentionPolicy: executor.Session.RP,
		Dialect:         string(executor.Session.Dialect),
		Query:           line,
	})
}
