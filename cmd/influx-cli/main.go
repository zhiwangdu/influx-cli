package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
	"github.com/zhiwangdu/influx-cli/internal/adapter/influxdb"
	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/ingest"
	"github.com/zhiwangdu/influx-cli/internal/render"
	"github.com/zhiwangdu/influx-cli/internal/repl"
	"github.com/zhiwangdu/influx-cli/internal/tui"
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
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRepl(cmd, flags)
		},
	}

	root.PersistentFlags().StringVar(&flags.configPath, "config", "", "config file path")
	root.PersistentFlags().StringVar(&flags.overrides.Profile, "profile", "", "config profile")
	root.PersistentFlags().StringVar(&flags.overrides.Adapter, "adapter", "", "adapter name: influxdb or opengemini")
	root.PersistentFlags().StringVar(&flags.overrides.Host, "host", "", "InfluxDB-compatible server host")
	root.PersistentFlags().IntVar(&flags.overrides.Port.Value, "port", 8086, "InfluxDB-compatible server port")
	root.PersistentFlags().BoolVar(&flags.overrides.SSL.Value, "ssl", false, "connect over HTTPS")
	root.PersistentFlags().BoolVar(&flags.overrides.UnsafeSSL.Value, "unsafeSsl", false, "skip HTTPS certificate verification")
	root.PersistentFlags().StringVar(&flags.overrides.Username, "username", "", "username")
	root.PersistentFlags().StringVar(&flags.overrides.Password, "password", "", "password")
	root.PersistentFlags().StringVar(&flags.overrides.Token, "token", "", "auth token")
	root.PersistentFlags().StringVar(&flags.overrides.Database, "db", "", "database")
	root.PersistentFlags().StringVar(&flags.overrides.RetentionPolicy, "rp", "", "retention policy")
	root.PersistentFlags().StringVar(&flags.overrides.Precision, "precision", "", "time precision: rfc3339, h, m, s, ms, u, ns")
	root.PersistentFlags().StringVar(&flags.overrides.Render, "format", "", "render format: auto, table, sparkline, chart, json")
	root.PersistentFlags().StringVar(&flags.overrides.Timeout, "timeout", "", "query timeout")
	root.PersistentFlags().IntVar(&flags.width, "width", 0, "render width")
	root.PersistentFlags().IntVar(&flags.maxRows, "max-rows", 200, "maximum table rows to print")
	root.PersistentFlags().IntVar(&flags.maxSeries, "max-series", 5, "maximum series to print")

	root.AddCommand(newQueryCommand(flags))
	root.AddCommand(newReplCommand(flags))
	root.AddCommand(newTUICommand(flags))
	root.AddCommand(newIngestCommand(flags))
	root.AddCommand(newConfigCommand(flags))
	return root
}

func newQueryCommand(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "query <influxql>",
		Short: "Execute a single query",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			effective, err := resolveEffective(cmd, flags)
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
			return runRepl(cmd, flags)
		},
	}
}

func runRepl(cmd *cobra.Command, flags *globalFlags) error {
	effective, err := resolveEffective(cmd, flags)
	if err != nil {
		return err
	}
	executor, err := newExecutor(effective)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()
	return repl.Run(ctx, executor, cmd.InOrStdin(), cmd.OutOrStdout(), repl.Options{
		Render:  renderOptions(effective, flags),
		History: history.NewStore("", history.Options{}),
	})
}

func newTUICommand(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "tui",
		Short: "Start the full-screen query TUI",
		RunE: func(cmd *cobra.Command, args []string) error {
			effective, err := resolveEffective(cmd, flags)
			if err != nil {
				return err
			}
			executor, err := newExecutor(effective)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			return tui.Run(ctx, executor, cmd.InOrStdin(), cmd.OutOrStdout(), tui.Options{
				Render:        renderOptions(effective, flags),
				History:       history.NewStore("", history.Options{}),
				QueryTimeout:  effective.Timeout,
				WatchInterval: 5 * time.Second,
			})
		},
	}
}

type ingestCommandFlags struct {
	rate        string
	duration    string
	start       string
	file        string
	tick        string
	batchSize   int
	pointCount  int
	seriesCount int
	hosts       int
	pids        int
	ratio       float64
	measurement string
	dryRun      bool
}

func newIngestCommand(flags *globalFlags) *cobra.Command {
	ingestFlags := &ingestCommandFlags{
		rate:        "100/s",
		duration:    "1m",
		tick:        "10s",
		batchSize:   5000,
		pointCount:  100,
		seriesCount: 100000,
		hosts:       10,
		pids:        1000,
		ratio:       0.1,
	}
	command := &cobra.Command{
		Use:   "ingest <dataset>",
		Short: "Generate and write reproducible TSDB datasets",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			effective, err := resolveEffective(cmd, flags)
			if err != nil {
				return err
			}
			dataset, err := ingest.ParseDataset(args[0])
			if err != nil {
				return err
			}
			var ratePerSecond int
			var duration time.Duration
			var tick time.Duration
			if dataset != ingest.DatasetIQL {
				ratePerSecond, err = ingest.ParseRate(ingestFlags.rate)
				if err != nil {
					return err
				}
				duration, err = time.ParseDuration(ingestFlags.duration)
				if err != nil {
					return fmt.Errorf("parse duration %q: %w", ingestFlags.duration, err)
				}
				tick, err = time.ParseDuration(ingestFlags.tick)
				if err != nil {
					return fmt.Errorf("parse tick %q: %w", ingestFlags.tick, err)
				}
			}
			start, err := parseIngestStart(ingestFlags.start)
			if err != nil {
				return err
			}
			if dataset != ingest.DatasetIQL && strings.TrimSpace(effective.Database) == "" && !ingestFlags.dryRun {
				return errors.New("database is required for ingest; set --db or configure a profile database")
			}

			writer, err := newIngestWriter(effective, cmd.OutOrStdout(), ingestFlags.dryRun)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()

			summary, err := ingest.Run(ctx, writer, ingest.Options{
				Dataset:              dataset,
				Database:             effective.Database,
				RetentionPolicy:      effective.RetentionPolicy,
				Precision:            effective.Precision,
				RatePerSecond:        ratePerSecond,
				Duration:             duration,
				Tick:                 tick,
				BatchSize:            ingestFlags.batchSize,
				PointCount:           ingestFlags.pointCount,
				SeriesCount:          ingestFlags.seriesCount,
				Hosts:                ingestFlags.hosts,
				PIDs:                 ingestFlags.pids,
				Ratio:                ingestFlags.ratio,
				Measurement:          ingestFlags.measurement,
				Start:                start,
				IQLFile:              ingestFlags.file,
				ForceDatabase:        strings.TrimSpace(flags.overrides.Database) != "",
				ForceRetentionPolicy: strings.TrimSpace(flags.overrides.RetentionPolicy) != "",
				ForcePrecision:       strings.TrimSpace(flags.overrides.Precision) != "",
				ForceStart:           cmd.Flags().Changed("start"),
				ForceBatchSize:       cmd.Flags().Changed("batch-size"),
				AllowEmptyDatabase:   ingestFlags.dryRun,
			})
			if err != nil {
				return err
			}

			summaryWriter := cmd.OutOrStdout()
			if ingestFlags.dryRun {
				summaryWriter = cmd.ErrOrStderr()
			}
			printIngestSummary(summaryWriter, summary, ingestFlags.dryRun)
			return nil
		},
	}
	command.Flags().StringVar(&ingestFlags.rate, "rate", ingestFlags.rate, "points per simulated second, for example 100/s or 10k/s")
	command.Flags().StringVar(&ingestFlags.duration, "duration", ingestFlags.duration, "simulated time range to generate")
	command.Flags().StringVar(&ingestFlags.start, "start", "", "simulated range start time in RFC3339 or RFC3339Nano")
	command.Flags().StringVar(&ingestFlags.file, "file", "", "iql file to generate mock data from")
	command.Flags().StringVar(&ingestFlags.tick, "tick", ingestFlags.tick, "timestamp step between stress-basic point groups")
	command.Flags().IntVar(&ingestFlags.batchSize, "batch-size", ingestFlags.batchSize, fmt.Sprintf("line protocol rows per write request (max %d)", ingest.MaxBatchSize))
	command.Flags().IntVar(&ingestFlags.pointCount, "point-count", ingestFlags.pointCount, "number of timestamp groups for stress-basic")
	command.Flags().IntVar(&ingestFlags.seriesCount, "series-count", ingestFlags.seriesCount, "number of generated series for stress-basic")
	command.Flags().IntVar(&ingestFlags.hosts, "hosts", ingestFlags.hosts, "number of host tag values")
	command.Flags().IntVar(&ingestFlags.pids, "pids", ingestFlags.pids, "number of pid tag values for high-cardinality data")
	command.Flags().Float64Var(&ingestFlags.ratio, "ratio", ingestFlags.ratio, "out-of-order row ratio from 0 to 1")
	command.Flags().StringVar(&ingestFlags.measurement, "measurement", "", "override the default measurement name")
	command.Flags().BoolVar(&ingestFlags.dryRun, "dry-run", false, "print generated line protocol instead of writing it")
	return command
}

func parseIngestStart(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	start, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse start %q: %w", raw, err)
	}
	return start, nil
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
			effective, err := resolveEffective(cmd, flags)
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

func newIngestWriter(effective config.Effective, out io.Writer, dryRun bool) (adapter.LineProtocolWriter, error) {
	if dryRun {
		return ingest.NewDryRunWriter(out), nil
	}
	executor, err := newExecutor(effective)
	if err != nil {
		return nil, err
	}
	writer, ok := executor.Adapter.(adapter.LineProtocolWriter)
	if !ok {
		return nil, fmt.Errorf("adapter %q does not support line protocol writes", effective.Adapter)
	}
	return writer, nil
}

func newExecutor(effective config.Effective) (*app.Executor, error) {
	client := newHTTPClient(effective.Timeout, effective.SSL, effective.UnsafeSSL)
	adapterName := strings.ToLower(effective.Adapter)
	switch adapterName {
	case "influxdb", "opengemini":
		adapter, err := influxdb.New(influxdb.Config{
			Name:            adapterName,
			Host:            effective.Host,
			Port:            effective.Port,
			SSL:             effective.SSL,
			UnsafeSSL:       effective.UnsafeSSL,
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
		session.SetAdapterName(adapter.Name())
		return app.NewExecutor(session, adapter), nil
	default:
		return nil, fmt.Errorf("unsupported adapter %q", effective.Adapter)
	}
}

func resolveEffective(cmd *cobra.Command, flags *globalFlags) (config.Effective, error) {
	overrides := flags.overrides
	overrides.Port.Set = flagChanged(cmd, "port")
	overrides.SSL.Set = flagChanged(cmd, "ssl")
	overrides.UnsafeSSL.Set = flagChanged(cmd, "unsafeSsl")
	return config.Resolve(flags.configPath, overrides, os.Getenv)
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flag(name)
	return flag != nil && flag.Changed
}

func newHTTPClient(timeout time.Duration, ssl, unsafeSSL bool) *http.Client {
	client := &http.Client{Timeout: timeout}
	if ssl && unsafeSSL {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		client.Transport = transport
	}
	return client
}

func printIngestSummary(w io.Writer, summary ingest.Summary, dryRun bool) {
	mode := "written"
	if dryRun {
		mode = "generated"
	}
	fmt.Fprintf(w, "ingest: %s\n", mode)
	fmt.Fprintf(w, "dataset: %s\n", summary.Dataset)
	fmt.Fprintf(w, "database: %s\n", printableSummaryValue(summary.Database))
	fmt.Fprintf(w, "retention_policy: %s\n", printableSummaryValue(summary.RetentionPolicy))
	fmt.Fprintf(w, "measurement: %s\n", summary.Measurement)
	if summary.Dataset == ingest.DatasetIQL {
		fmt.Fprintf(w, "iql_file: %s\n", summary.IQLFile)
	}
	fmt.Fprintf(w, "precision: %s\n", summary.Precision)
	if summary.Dataset != ingest.DatasetStressBasic && summary.Dataset != ingest.DatasetIQL {
		fmt.Fprintf(w, "rate: %d/s\n", summary.RatePerSecond)
	}
	fmt.Fprintf(w, "duration: %s\n", summary.Duration)
	if summary.Dataset == ingest.DatasetStressBasic {
		fmt.Fprintf(w, "point_count: %d\n", summary.PointCount)
		fmt.Fprintf(w, "series_count: %d\n", summary.SeriesCount)
		fmt.Fprintf(w, "tick: %s\n", summary.Tick)
	}
	if summary.Dataset == ingest.DatasetIQL {
		fmt.Fprintf(w, "insert_statements: %d\n", summary.IQLInserts)
		fmt.Fprintf(w, "skipped_queries: %d\n", summary.IQLSkippedQuery)
		fmt.Fprintf(w, "skipped_influxql: %d\n", summary.IQLSkippedRaw)
		if len(summary.IQLIgnoredSets) > 0 {
			fmt.Fprintf(w, "ignored_iql_settings: %s\n", strings.Join(summary.IQLIgnoredSets, ", "))
		}
	}
	fmt.Fprintf(w, "points: %d\n", summary.WrittenPoints)
	fmt.Fprintf(w, "batches: %d\n", summary.Batches)
	fmt.Fprintf(w, "simulated_range: %s to %s\n", summary.StartedAt.Format(time.RFC3339Nano), summary.EndedAt.Format(time.RFC3339Nano))
	if (summary.Dataset == ingest.DatasetStressBasic || summary.Dataset == ingest.DatasetIQL) && !summary.DataStartedAt.IsZero() && !summary.DataEndedAt.IsZero() &&
		(!summary.DataStartedAt.Equal(summary.StartedAt) || !summary.DataEndedAt.Equal(summary.EndedAt)) {
		fmt.Fprintf(w, "data_range: %s to %s\n", summary.DataStartedAt.Format(time.RFC3339Nano), summary.DataEndedAt.Format(time.RFC3339Nano))
	}
	fmt.Fprintf(w, "elapsed: %s\n", summary.Elapsed.Truncate(time.Millisecond))
}

func renderOptions(effective config.Effective, flags *globalFlags) render.Options {
	width := flags.width
	if width <= 0 {
		width = envInt("COLUMNS", 80)
	}
	return render.Options{
		Format:    firstNonEmpty(effective.Render, render.FormatTable),
		Width:     width,
		MaxRows:   flags.maxRows,
		MaxSeries: flags.maxSeries,
		Color:     colorEnabled(),
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

func printableSummaryValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func init() {
	cobra.EnableCommandSorting = false
}
