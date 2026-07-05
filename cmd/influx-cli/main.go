package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/zhiwangdu/influx-cli/internal/adapter/influxdb"
	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
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
				Render: renderOptions(effective, flags),
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

func init() {
	cobra.EnableCommandSorting = false
}
