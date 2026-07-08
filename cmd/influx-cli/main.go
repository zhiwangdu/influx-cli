package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/zhiwangdu/influx-cli/internal/adapter/influxdb"
	"github.com/zhiwangdu/influx-cli/internal/analysis/storage"
	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/render"
	"github.com/zhiwangdu/influx-cli/internal/repl"
)

type globalFlags struct {
	configPath string
	overrides  config.Overrides
	width      int
	maxRows    int
	maxSeries  int
}

func main() {
	if err := newRootCommand().Execute(); err != nil {
		var reported reportedError
		if !errors.As(err, &reported) {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

type reportedError struct {
	err error
}

func (e reportedError) Error() string {
	return e.err.Error()
}

func (e reportedError) Unwrap() error {
	return e.err
}

func newRootCommand() *cobra.Command {
	flags := &globalFlags{}
	root := &cobra.Command{
		Use:           "influx-cli",
		Short:         "TSDB-native terminal query console for InfluxDB-compatible endpoints",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().StringVar(&flags.configPath, "config", "", "config file path")
	root.PersistentFlags().StringVar(&flags.overrides.Profile, "profile", "", "config profile")
	root.PersistentFlags().StringVar(&flags.overrides.Adapter, "adapter", "", "adapter name: influxdb or opengemini")
	root.PersistentFlags().StringVar(&flags.overrides.URL, "url", "", "InfluxDB-compatible server URL")
	root.PersistentFlags().StringVar(&flags.overrides.Username, "username", "", "username")
	root.PersistentFlags().StringVar(&flags.overrides.Password, "password", "", "password")
	root.PersistentFlags().StringVar(&flags.overrides.Token, "token", "", "auth token")
	root.PersistentFlags().StringVar(&flags.overrides.Database, "db", "", "database")
	root.PersistentFlags().StringVar(&flags.overrides.RetentionPolicy, "rp", "", "retention policy")
	root.PersistentFlags().StringVar(&flags.overrides.Precision, "precision", "", "time precision: rfc3339, h, m, s, ms, u, ns")
	root.PersistentFlags().StringVar(&flags.overrides.Render, "format", "", "render format: auto, table, sparkline, json")
	root.PersistentFlags().StringVar(&flags.overrides.Timeout, "timeout", "", "query timeout")
	root.PersistentFlags().IntVar(&flags.width, "width", 0, "render width")
	root.PersistentFlags().IntVar(&flags.maxRows, "max-rows", 200, "maximum table rows to print")
	root.PersistentFlags().IntVar(&flags.maxSeries, "max-series", 5, "maximum series to print")

	root.AddCommand(newQueryCommand(flags))
	root.AddCommand(newReplCommand(flags))
	root.AddCommand(newConfigCommand(flags))
	root.AddCommand(newStorageCommand(flags))
	return root
}

func newQueryCommand(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "query <influxql>",
		Short: "Execute a single query",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			effective, err := config.Resolve(flags.configPath, flags.overrides, os.Getenv)
			if err != nil {
				return err
			}
			executor, err := newExecutor(effective)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			ctx, cancel := context.WithTimeout(ctx, effective.Timeout)
			defer cancel()

			options := renderOptions(effective, flags)
			res, err := executor.Execute(ctx, strings.Join(args, " "))
			if err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "error:", err)
				fmt.Fprintln(cmd.ErrOrStderr(), render.RenderStatusLine(executor.Session.StatusLine(), options))
				return reportedError{err: err}
			}
			output, _, err := render.Render(res, options)
			if err != nil {
				return err
			}
			if strings.TrimSpace(output) != "" {
				fmt.Fprintln(cmd.OutOrStdout(), output)
			}
			fmt.Fprintln(cmd.OutOrStdout(), render.RenderStatusLine(executor.Session.StatusLine(), options))
			return nil
		},
	}
}

func newReplCommand(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "repl",
		Short: "Start an interactive query REPL",
		RunE: func(cmd *cobra.Command, args []string) error {
			effective, err := config.Resolve(flags.configPath, flags.overrides, os.Getenv)
			if err != nil {
				return err
			}
			executor, err := newExecutor(effective)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			return repl.Run(ctx, executor, os.Stdin, cmd.OutOrStdout(), repl.Options{
				Render:  renderOptions(effective, flags),
				History: history.NewStore("", history.Options{}),
			})
		},
	}
}

func newConfigCommand(flags *globalFlags) *cobra.Command {
	configCommand := &cobra.Command{
		Use:   "config",
		Short: "Inspect configuration",
	}
	configCommand.AddCommand(&cobra.Command{
		Use:   "path",
		Short: "Print the config path",
		RunE: func(cmd *cobra.Command, args []string) error {
			path := flags.configPath
			if path == "" {
				path = config.DefaultPath()
			}
			fmt.Fprintln(cmd.OutOrStdout(), path)
			return nil
		},
	})
	configCommand.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration with secrets redacted",
		RunE: func(cmd *cobra.Command, args []string) error {
			effective, err := config.Resolve(flags.configPath, flags.overrides, os.Getenv)
			if err != nil {
				return err
			}
			for _, line := range effective.RedactedLines() {
				fmt.Fprintln(cmd.OutOrStdout(), line)
			}
			return nil
		},
	})
	return configCommand
}

type storageAnalyzeFlags struct {
	format       string
	report       bool
	recursive    bool
	from         string
	to           string
	keys         []string
	seriesIDs    []string
	metaIndexIDs []string
	columns      []string
	fields       []string
	anyFields    []string
	noneFields   []string
	measurements []string
	tags         []string
	cursorOrder  string
	sampleKeys   int
	maxBlocks    int
}

func newStorageCommand(flags *globalFlags) *cobra.Command {
	storageCommand := &cobra.Command{
		Use:   "storage",
		Short: "Analyze local TSDB storage files",
	}

	analyzeFlags := &storageAnalyzeFlags{
		format:     string(storage.FormatAuto),
		sampleKeys: 5,
		maxBlocks:  50,
	}
	analyzeCommand := &cobra.Command{
		Use:   "analyze <file-or-dir>...",
		Short: "Inspect InfluxDB TSM/TSI and openGemini TSSP file metadata",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queryRange, err := parseStorageRange(analyzeFlags.from, analyzeFlags.to)
			if err != nil {
				return err
			}
			storageFormat := storage.Format(strings.ToLower(analyzeFlags.format))
			if hasNonEmptyValues(analyzeFlags.keys) && !queryRange.Set && storageFormat != storage.FormatMergeset {
				return fmt.Errorf("--key requires --from and --to because decode-path planning needs a query range")
			}
			seriesIDs, err := parseStorageSeriesIDs(analyzeFlags.seriesIDs)
			if err != nil {
				return err
			}
			if len(seriesIDs) > 0 && !queryRange.Set && storageFormat != storage.FormatSeriesFile {
				return fmt.Errorf("--series-id requires --from and --to because decode-path planning needs a query range")
			}
			metaIndexIDs, err := parseStorageMetaIndexIDs(analyzeFlags.metaIndexIDs)
			if err != nil {
				return err
			}
			if len(metaIndexIDs) > 0 && !queryRange.Set {
				return fmt.Errorf("--meta-index-id requires --from and --to because decode-path planning needs a query range")
			}
			if hasNonEmptyValues(analyzeFlags.columns) && !queryRange.Set {
				return fmt.Errorf("--column requires --from and --to because TSSP data ReadAt planning needs a query range")
			}
			fieldFilters, err := parseStorageFieldFiltersForFlag("--field", analyzeFlags.fields)
			if err != nil {
				return err
			}
			if len(fieldFilters) > 0 && !queryRange.Set {
				return fmt.Errorf("--field requires --from and --to because TSSP record filtering needs a query range")
			}
			anyFieldFilters, err := parseStorageFieldFiltersForFlag("--field-any", analyzeFlags.anyFields)
			if err != nil {
				return err
			}
			if len(anyFieldFilters) > 0 && !queryRange.Set {
				return fmt.Errorf("--field-any requires --from and --to because TSSP record filtering needs a query range")
			}
			noneFieldFilters, err := parseStorageFieldFiltersForFlag("--field-none", analyzeFlags.noneFields)
			if err != nil {
				return err
			}
			if len(noneFieldFilters) > 0 && !queryRange.Set {
				return fmt.Errorf("--field-none requires --from and --to because TSSP record filtering needs a query range")
			}
			tagFilters, err := parseStorageTagFilters(analyzeFlags.tags)
			if err != nil {
				return err
			}
			cursorDescending, err := parseStorageCursorDescending(analyzeFlags.cursorOrder)
			if err != nil {
				return err
			}
			report, err := storage.Analyze(cmd.Context(), args, storage.Options{
				Format:            storageFormat,
				Recursive:         analyzeFlags.recursive,
				KeySampleLimit:    analyzeFlags.sampleKeys,
				BlockSampleLimit:  analyzeFlags.maxBlocks,
				QueryRange:        queryRange,
				QueryKeys:         analyzeFlags.keys,
				QuerySeriesIDs:    seriesIDs,
				QueryMetaIndexIDs: metaIndexIDs,
				QueryColumns:      analyzeFlags.columns,
				QueryFields:       fieldFilters,
				QueryAnyFields:    anyFieldFilters,
				QueryNoneFields:   noneFieldFilters,
				QueryMeasurements: analyzeFlags.measurements,
				QueryTags:         tagFilters,
				CursorDescending:  cursorDescending,
			})
			if err != nil {
				return fmt.Errorf("storage analyze: %w", err)
			}

			if analyzeFlags.report {
				fmt.Fprint(cmd.OutOrStdout(), report.Markdown())
				return nil
			}

			outputFormat := firstNonEmpty(flags.overrides.Render, render.FormatTable)
			normalized, err := render.NormalizeFormat(outputFormat)
			if err != nil {
				return err
			}
			if normalized == render.FormatAuto {
				normalized = render.FormatTable
			}
			switch normalized {
			case render.FormatJSON:
				body, err := json.MarshalIndent(report, "", "  ")
				if err != nil {
					return fmt.Errorf("render storage json: %w", err)
				}
				fmt.Fprintln(cmd.OutOrStdout(), string(body))
			case render.FormatTable:
				options := render.Options{
					Format:    render.FormatTable,
					Width:     renderWidth(flags),
					MaxRows:   flags.maxRows,
					MaxSeries: flags.maxSeries,
					Color:     colorEnabled(),
				}
				output, _, err := render.Render(report.Result(), options)
				if err != nil {
					return err
				}
				if strings.TrimSpace(output) != "" {
					fmt.Fprintln(cmd.OutOrStdout(), output)
				}
				if len(report.Notices) > 0 {
					fmt.Fprintf(cmd.ErrOrStderr(), "warning: storage analyzer produced %d notice(s); use --format json for notice details\n", len(report.Notices))
				}
			default:
				return fmt.Errorf("storage analyze supports table or json output, got %q", normalized)
			}
			return nil
		},
	}
	analyzeCommand.Flags().StringVar(&analyzeFlags.format, "storage-format", analyzeFlags.format, "storage file format: auto, tsm, wal, tssp, tssp-metaindex, tsi, tsi-log, series-file, fields-index, mergeset, opengemini-meta, opengemini-pk-meta, opengemini-pk-index, opengemini-bloom-filter, opengemini-text-index (skipped)")
	analyzeCommand.Flags().BoolVar(&analyzeFlags.report, "report", false, "render a count-oriented markdown storage diagnostic report for issue or PR sharing")
	analyzeCommand.Flags().BoolVar(&analyzeFlags.recursive, "recursive", false, "walk directories recursively")
	analyzeCommand.Flags().StringVar(&analyzeFlags.from, "from", "", "query range start as RFC3339 or unix nanoseconds")
	analyzeCommand.Flags().StringVar(&analyzeFlags.to, "to", "", "query range end as RFC3339 or unix nanoseconds")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.keys, "key", nil, "TSM index key or mergeset item key to include in query/search planning; repeat for multiple keys")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.seriesIDs, "series-id", nil, "series ID to inspect; for TSSP it also participates in query decode-path planning and requires --from/--to")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.metaIndexIDs, "meta-index-id", nil, "openGemini detached TSSP meta-index ID to include in query decode-path planning; repeat for multiple IDs")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.columns, "column", nil, "TSSP column name to project during local data ReadAt planning and block probes; repeat for multiple columns; requires --from/--to")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.fields, "field", nil, "TSSP decoded field predicate as key=value, key==value, key equals/equal value, key!=value, key<>value, key not-equals/not equals/not_equals value, key not = value, key not == value, key !equals/!equal value, key exists, key not-exists, key !exists, key=~<pattern>, key!~<pattern>, key matches/match/regex/regexp <pattern> and not/! variants, key is value, key is-not value, key is not value, key>value, key>=value, key<value, key<=value, key !> value, key !>= value, key !< value, key !<= value, key not > value, key not >= value, key not < value, key not <= value as inverse ordered comparison aliases, key in (value1,value2), key not-in/!in (value1,value2), key between (lower,upper), key not-between/!between (lower,upper), key contains/icontains value, key not-contains/!contains or not-icontains/!icontains value, key like/ilike pattern, key not-like/!like or not-ilike/!ilike pattern, key starts-with/istarts-with value, key not-starts-with/!starts-with or not-istarts-with/!istarts-with value, key ends-with/iends-with value, or key not-ends-with/!ends-with or not-iends-with/!iends-with value for local record filtering, including decoded time when present; multi-word operators also accept hyphen, space, or underscore separators; range parentheses are optional; quote string values that contain commas or parentheses; repeat for multiple fields; requires --from/--to")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.anyFields, "field-any", nil, "TSSP decoded field predicate using the same syntax as --field; at least one repeated --field-any predicate must match; combines with --field as required AND predicates; requires --from/--to")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.noneFields, "field-none", nil, "TSSP decoded field predicate using the same syntax as --field; no repeated --field-none predicate may match; combines after required AND and OR predicates; requires --from/--to")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.measurements, "measurement", nil, "TSI measurement name to inspect; repeat for multiple measurements")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.tags, "tag", nil, "TSI tag predicate as key=value; repeat for multiple tags")
	analyzeCommand.Flags().StringVar(&analyzeFlags.cursorOrder, "cursor-order", "asc", "TSM/openGemini TSSP cursor order for decode-path planning: asc or desc")
	analyzeCommand.Flags().IntVar(&analyzeFlags.sampleKeys, "sample-keys", analyzeFlags.sampleKeys, "maximum key or series ID samples per file")
	analyzeCommand.Flags().IntVar(&analyzeFlags.maxBlocks, "max-blocks", analyzeFlags.maxBlocks, "maximum block samples per file")

	storageCommand.AddCommand(analyzeCommand)
	return storageCommand
}

func newExecutor(effective config.Effective) (*app.Executor, error) {
	client := &http.Client{Timeout: effective.Timeout}
	adapterName := strings.ToLower(effective.Adapter)
	switch adapterName {
	case "influxdb", "opengemini":
		adapter, err := influxdb.New(influxdb.Config{
			Name:            adapterName,
			URL:             effective.URL,
			Username:        effective.Username,
			Password:        effective.Password,
			Token:           effective.Token,
			Database:        effective.Database,
			RetentionPolicy: effective.RetentionPolicy,
			Precision:       effective.Precision,
			HTTPClient:      client,
		})
		if err != nil {
			return nil, err
		}
		session := app.NewSession(effective)
		session.AdapterName = adapter.Name()
		return app.NewExecutor(session, adapter), nil
	default:
		return nil, fmt.Errorf("unsupported adapter %q", effective.Adapter)
	}
}

func renderOptions(effective config.Effective, flags *globalFlags) render.Options {
	return render.Options{
		Format:    firstNonEmpty(effective.Render, render.FormatTable),
		Width:     renderWidth(flags),
		MaxRows:   flags.maxRows,
		MaxSeries: flags.maxSeries,
		Color:     colorEnabled(),
	}
}

func renderWidth(flags *globalFlags) int {
	width := flags.width
	if width <= 0 {
		width = envInt("COLUMNS", 80)
	}
	return width
}

func parseStorageRange(from, to string) (storage.TimeRange, error) {
	if strings.TrimSpace(from) == "" && strings.TrimSpace(to) == "" {
		return storage.TimeRange{}, nil
	}
	if strings.TrimSpace(from) == "" || strings.TrimSpace(to) == "" {
		return storage.TimeRange{}, fmt.Errorf("both --from and --to are required when filtering by query range")
	}
	minTime, err := parseStorageTime(from)
	if err != nil {
		return storage.TimeRange{}, fmt.Errorf("parse --from: %w", err)
	}
	maxTime, err := parseStorageTime(to)
	if err != nil {
		return storage.TimeRange{}, fmt.Errorf("parse --to: %w", err)
	}
	return storage.NewTimeRange(minTime, maxTime)
}

func parseStorageTime(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UnixNano(), nil
	}
	return 0, fmt.Errorf("use RFC3339/RFC3339Nano or unix nanoseconds")
}

func parseStorageTagFilters(values []string) ([]storage.TagFilter, error) {
	if len(values) == 0 {
		return nil, nil
	}
	filters := make([]storage.TagFilter, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key, tagValue, ok := strings.Cut(trimmed, "=")
		if !ok {
			return nil, fmt.Errorf("parse --tag %q: use key=value", value)
		}
		filter := storage.TagFilter{
			Key:   strings.TrimSpace(key),
			Value: strings.TrimSpace(tagValue),
		}
		if filter.Key == "" {
			return nil, fmt.Errorf("parse --tag %q: key cannot be empty", value)
		}
		filters = append(filters, filter)
	}
	return filters, nil
}

func parseStorageFieldFilters(values []string) ([]storage.FieldFilter, error) {
	return parseStorageFieldFiltersForFlag("--field", values)
}

func parseStorageFieldFiltersForFlag(flag string, values []string) ([]storage.FieldFilter, error) {
	if len(values) == 0 {
		return nil, nil
	}
	filters := make([]storage.FieldFilter, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key, op, filterValue, ok := splitStorageFieldFilter(trimmed)
		if !ok {
			return nil, fmt.Errorf("parse %s %q: use key=value, key==value, key equals/equal value, key!=value, key<>value, key not-equals/not equals/not_equals value, key not = value, key not == value, key !equals/!equal value, key exists, key not-exists, key !exists, key=~<pattern>, key!~<pattern>, key matches/match/regex/regexp <pattern> and not/! variants, key is value, key is-not value, key is not value, key>value, key>=value, key<value, key<=value, key !> value, key !>= value, key !< value, key !<= value, key not > value, key not >= value, key not < value, key not <= value as inverse ordered comparison aliases, key in (value1,value2), key not-in (value1,value2), key !in (value1,value2), key between (lower,upper), key not-between (lower,upper), key !between (lower,upper), key contains/icontains value, key not-contains/not-icontains value, key !contains/!icontains value, key like/ilike pattern, key not-like/not-ilike pattern, key !like/!ilike pattern, key starts-with/istarts-with value, key not-starts-with/not-istarts-with value, key !starts-with/!istarts-with value, key ends-with/iends-with value, key not-ends-with/not-iends-with value, or key !ends-with/!iends-with value; multi-word operators also accept hyphen, space, or underscore separators; range parentheses are optional", flag, value)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("parse %s %q: key cannot be empty", flag, value)
		}
		filterValue = strings.TrimSpace(filterValue)
		if filterValue == "" && op != "=" && op != "==" && op != "!=" && op != "<>" && op != "exists" && op != "not-exists" {
			return nil, fmt.Errorf("parse %s %q: value cannot be empty for operator %s", flag, value, op)
		}
		if op == "=" || op == "==" {
			op = ""
		} else if op == "<>" {
			op = "!="
		} else if op == "!>" {
			op = "<="
		} else if op == "!>=" {
			op = "<"
		} else if op == "!<" {
			op = ">="
		} else if op == "!<=" {
			op = ">"
		}
		filters = append(filters, storage.FieldFilter{
			Key:   key,
			Op:    op,
			Value: filterValue,
		})
	}
	return filters, nil
}

func splitStorageFieldFilter(value string) (string, string, string, bool) {
	bestIndex := len(value) + 1
	bestOp := ""
	for _, op := range []string{"!>=", "!<=", "!>", "!<", ">=", "<=", "==", "!=", "<>", "=~", "!~", "=", ">", "<"} {
		searchFrom := 0
		for {
			relativeIndex := strings.Index(value[searchFrom:], op)
			if relativeIndex < 0 {
				break
			}
			index := searchFrom + relativeIndex
			searchFrom = index + 1
			if storageFieldOperatorInLiteral(value, index) {
				continue
			}
			if index < bestIndex || index == bestIndex && len(op) > len(bestOp) {
				bestIndex = index
				bestOp = op
			}
		}
	}
	if bestOp != "" {
		wordKey, wordOp, wordValue, wordIndex, wordOK := splitStorageFieldWordFilter(value)
		if wordOK && wordIndex < bestIndex && storageFieldWordOperatorCanPrecedeLaterSymbol(wordOp, wordValue) {
			return wordKey, wordOp, wordValue, true
		}
		return value[:bestIndex], bestOp, value[bestIndex+len(bestOp):], true
	}
	wordKey, wordOp, wordValue, _, wordOK := splitStorageFieldWordFilter(value)
	if wordOK {
		return wordKey, wordOp, wordValue, true
	}
	return "", "", "", false
}

func splitStorageFieldWordFilter(value string) (string, string, string, int, bool) {
	for _, operator := range []struct {
		text     string
		op       string
		terminal bool
	}{
		{text: "not-between", op: "not-between"},
		{text: "not between", op: "not-between"},
		{text: "not_between", op: "not-between"},
		{text: "!between", op: "not-between"},
		{text: "not-equals", op: "!="},
		{text: "not equals", op: "!="},
		{text: "not_equals", op: "!="},
		{text: "not-equal", op: "!="},
		{text: "not equal", op: "!="},
		{text: "not_equal", op: "!="},
		{text: "not ==", op: "!="},
		{text: "not =", op: "!="},
		{text: "!equals", op: "!="},
		{text: "!equal", op: "!="},
		{text: "not->=", op: "!>="},
		{text: "not_>=", op: "!>="},
		{text: "not >=", op: "!>="},
		{text: "not->", op: "!>"},
		{text: "not_>", op: "!>"},
		{text: "not >", op: "!>"},
		{text: "not-<=", op: "!<="},
		{text: "not_<=", op: "!<="},
		{text: "not <=", op: "!<="},
		{text: "not-<", op: "!<"},
		{text: "not_<", op: "!<"},
		{text: "not <", op: "!<"},
		{text: "not-starts-with", op: "not-starts-with"},
		{text: "not starts with", op: "not-starts-with"},
		{text: "not_starts_with", op: "not-starts-with"},
		{text: "!starts-with", op: "not-starts-with"},
		{text: "!starts_with", op: "not-starts-with"},
		{text: "not-ends-with", op: "not-ends-with"},
		{text: "not ends with", op: "not-ends-with"},
		{text: "not_ends_with", op: "not-ends-with"},
		{text: "!ends-with", op: "not-ends-with"},
		{text: "!ends_with", op: "not-ends-with"},
		{text: "not-contains", op: "not-contains"},
		{text: "not contains", op: "not-contains"},
		{text: "not_contains", op: "not-contains"},
		{text: "!contains", op: "not-contains"},
		{text: "not-icontains", op: "not-icontains"},
		{text: "not icontains", op: "not-icontains"},
		{text: "not_icontains", op: "not-icontains"},
		{text: "!icontains", op: "not-icontains"},
		{text: "not-like", op: "not-like"},
		{text: "not like", op: "not-like"},
		{text: "not_like", op: "not-like"},
		{text: "!like", op: "not-like"},
		{text: "not-ilike", op: "not-ilike"},
		{text: "not ilike", op: "not-ilike"},
		{text: "not_ilike", op: "not-ilike"},
		{text: "!ilike", op: "not-ilike"},
		{text: "not-matches", op: "!~"},
		{text: "not matches", op: "!~"},
		{text: "not_matches", op: "!~"},
		{text: "!matches", op: "!~"},
		{text: "not-match", op: "!~"},
		{text: "not match", op: "!~"},
		{text: "not_match", op: "!~"},
		{text: "!match", op: "!~"},
		{text: "not-regex", op: "!~"},
		{text: "not regex", op: "!~"},
		{text: "not_regex", op: "!~"},
		{text: "!regex", op: "!~"},
		{text: "not-regexp", op: "!~"},
		{text: "not regexp", op: "!~"},
		{text: "not_regexp", op: "!~"},
		{text: "!regexp", op: "!~"},
		{text: "not-exists", op: "not-exists", terminal: true},
		{text: "not exists", op: "not-exists", terminal: true},
		{text: "not_exists", op: "not-exists", terminal: true},
		{text: "!exists", op: "not-exists", terminal: true},
		{text: "not-in", op: "not-in"},
		{text: "not in", op: "not-in"},
		{text: "not_in", op: "not-in"},
		{text: "!in", op: "not-in"},
		{text: "not-istarts-with", op: "not-istarts-with"},
		{text: "not istarts with", op: "not-istarts-with"},
		{text: "not_istarts_with", op: "not-istarts-with"},
		{text: "!istarts-with", op: "not-istarts-with"},
		{text: "!istarts_with", op: "not-istarts-with"},
		{text: "not-iends-with", op: "not-iends-with"},
		{text: "not iends with", op: "not-iends-with"},
		{text: "not_iends_with", op: "not-iends-with"},
		{text: "!iends-with", op: "not-iends-with"},
		{text: "!iends_with", op: "not-iends-with"},
		{text: "is-not", op: "!="},
		{text: "is not", op: "!="},
		{text: "is_not", op: "!="},
		{text: "is", op: "="},
		{text: "equals", op: "="},
		{text: "equal", op: "="},
		{text: "between", op: "between"},
		{text: "istarts-with", op: "istarts-with"},
		{text: "istarts with", op: "istarts-with"},
		{text: "istarts_with", op: "istarts-with"},
		{text: "starts-with", op: "starts-with"},
		{text: "starts with", op: "starts-with"},
		{text: "starts_with", op: "starts-with"},
		{text: "iends-with", op: "iends-with"},
		{text: "iends with", op: "iends-with"},
		{text: "iends_with", op: "iends-with"},
		{text: "ends-with", op: "ends-with"},
		{text: "ends with", op: "ends-with"},
		{text: "ends_with", op: "ends-with"},
		{text: "icontains", op: "icontains"},
		{text: "contains", op: "contains"},
		{text: "ilike", op: "ilike"},
		{text: "like", op: "like"},
		{text: "matches", op: "=~"},
		{text: "match", op: "=~"},
		{text: "regex", op: "=~"},
		{text: "regexp", op: "=~"},
		{text: "exists", op: "exists", terminal: true},
		{text: "in", op: "in"},
	} {
		key, filterValue, index, ok := splitStorageFieldWordOperator(value, operator.text, operator.terminal)
		if ok {
			return key, operator.op, filterValue, index, true
		}
	}
	return "", "", "", 0, false
}

func storageFieldWordOperatorCanPrecedeLaterSymbol(op, value string) bool {
	switch op {
	case "=", "!=", "=~", "!~", "!>", "!>=", "!<", "!<=", "contains", "not-contains", "icontains", "not-icontains", "like", "not-like", "ilike", "not-ilike", "starts-with", "not-starts-with", "istarts-with", "not-istarts-with", "ends-with", "not-ends-with", "iends-with", "not-iends-with":
		return true
	case "in", "not-in", "between", "not-between":
		value = strings.TrimSpace(value)
		return strings.HasPrefix(value, "(") || strings.Contains(value, ",")
	default:
		return false
	}
}

func splitStorageFieldWordOperator(value, operator string, terminal bool) (string, string, int, bool) {
	lower := strings.ToLower(value)
	if terminal {
		needle := " " + operator
		if strings.HasSuffix(lower, needle) {
			index := len(value) - len(needle)
			if !storageFieldOperatorInLiteral(value, index) {
				key := value[:index]
				if !strings.ContainsAny(key, "=<>!") && !strings.EqualFold(strings.TrimSpace(key), "not") && !storageFieldTerminalOperatorShadowsValueOperator(key) {
					return key, "", index, true
				}
			}
		}
	}
	for _, needle := range []string{" " + operator + " ", " " + operator + "("} {
		searchFrom := 0
		for {
			relativeIndex := strings.Index(lower[searchFrom:], needle)
			if relativeIndex < 0 {
				break
			}
			index := searchFrom + relativeIndex
			searchFrom = index + len(needle)
			if storageFieldOperatorInLiteral(value, index) {
				continue
			}
			key := value[:index]
			if strings.ContainsAny(key, "=<>!") {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(key), "not") {
				continue
			}
			valueStart := index + 1 + len(operator)
			return key, strings.TrimSpace(value[valueStart:]), index, true
		}
	}
	return "", "", 0, false
}

func storageFieldTerminalOperatorShadowsValueOperator(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, operator := range []string{
		"not-between", "not between", "not_between", "!between",
		"not-starts-with", "not starts with", "not_starts_with", "!starts-with", "!starts_with",
		"not-istarts-with", "not istarts with", "not_istarts_with", "!istarts-with", "!istarts_with",
		"not-ends-with", "not ends with", "not_ends_with", "!ends-with", "!ends_with",
		"not-iends-with", "not iends with", "not_iends_with", "!iends-with", "!iends_with",
		"not-contains", "not contains", "not_contains", "!contains",
		"not-icontains", "not icontains", "not_icontains", "!icontains",
		"not-like", "not like", "not_like", "!like",
		"not-ilike", "not ilike", "not_ilike", "!ilike",
		"not-matches", "not matches", "not_matches", "!matches",
		"not-match", "not match", "not_match", "!match",
		"not-regex", "not regex", "not_regex", "!regex",
		"not-regexp", "not regexp", "not_regexp", "!regexp",
		"not-in", "not in", "not_in", "!in",
		"is-not", "is not", "is_not",
		"is", "between", "istarts-with", "istarts with", "istarts_with",
		"starts-with", "starts with", "starts_with",
		"iends-with", "iends with", "iends_with",
		"ends-with", "ends with", "ends_with", "icontains", "contains", "ilike", "like",
		"matches", "match", "regex", "regexp", "in",
	} {
		if strings.HasSuffix(key, " "+operator) {
			return true
		}
	}
	return false
}

func storageFieldOperatorInLiteral(value string, index int) bool {
	depth := 0
	var quote byte
	escaped := false
	for i := 0; i < index && i < len(value); i++ {
		c := value[i]
		if escaped {
			escaped = false
			continue
		}
		if quote != 0 {
			if c == '\\' {
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
	}
	return depth > 0 || quote != 0
}

func parseStorageSeriesIDs(values []string) ([]uint64, error) {
	return parseStorageUint64Filter("--series-id", values)
}

func parseStorageMetaIndexIDs(values []string) ([]uint64, error) {
	return parseStorageUint64Filter("--meta-index-id", values)
}

func parseStorageUint64Filter(flag string, values []string) ([]uint64, error) {
	if len(values) == 0 {
		return nil, nil
	}
	ids := make([]uint64, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		id, err := strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s %q: use an unsigned integer", flag, value)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func parseStorageCursorDescending(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "asc", "ascending":
		return false, nil
	case "desc", "descending":
		return true, nil
	default:
		return false, fmt.Errorf("parse --cursor-order %q: use asc or desc", value)
	}
}

func colorEnabled() bool {
	if _, disabled := os.LookupEnv("NO_COLOR"); disabled {
		return false
	}
	return strings.ToLower(os.Getenv("TERM")) != "dumb"
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func hasNonEmptyValues(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func init() {
	cobra.EnableCommandSorting = false
}
