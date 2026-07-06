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
	recursive    bool
	from         string
	to           string
	keys         []string
	seriesIDs    []string
	metaIndexIDs []string
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
				QueryMeasurements: analyzeFlags.measurements,
				QueryTags:         tagFilters,
				CursorDescending:  cursorDescending,
			})
			if err != nil {
				return fmt.Errorf("storage analyze: %w", err)
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
				for _, notice := range report.Notices {
					fmt.Fprintln(cmd.ErrOrStderr(), "warning:", notice)
				}
			default:
				return fmt.Errorf("storage analyze supports table or json output, got %q", normalized)
			}
			return nil
		},
	}
	analyzeCommand.Flags().StringVar(&analyzeFlags.format, "storage-format", analyzeFlags.format, "storage file format: auto, tsm, wal, tssp, tssp-metaindex, tsi, tsi-log, series-file, mergeset, opengemini-meta")
	analyzeCommand.Flags().BoolVar(&analyzeFlags.recursive, "recursive", false, "walk directories recursively")
	analyzeCommand.Flags().StringVar(&analyzeFlags.from, "from", "", "query range start as RFC3339 or unix nanoseconds")
	analyzeCommand.Flags().StringVar(&analyzeFlags.to, "to", "", "query range end as RFC3339 or unix nanoseconds")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.keys, "key", nil, "TSM index key or mergeset item key to include in query/search planning; repeat for multiple keys")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.seriesIDs, "series-id", nil, "series ID to inspect; for TSSP it also participates in query decode-path planning and requires --from/--to")
	analyzeCommand.Flags().StringArrayVar(&analyzeFlags.metaIndexIDs, "meta-index-id", nil, "openGemini detached TSSP meta-index ID to include in query decode-path planning; repeat for multiple IDs")
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
