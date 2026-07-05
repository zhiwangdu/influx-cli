package repl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/render"
)

type Options struct {
	Render render.Options
}

func Run(ctx context.Context, executor *app.Executor, in io.Reader, out io.Writer, options Options) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1024), 1024*1024)

	fmt.Fprintln(out, "Enter run | Ctrl+C cancel | :q quit | :help commands")
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fmt.Fprint(out, executor.Session.Prompt())
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			return nil
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == ":q" || strings.EqualFold(line, "q") {
			return nil
		}
		// Render format is REPL-local UI state, so it is handled before app
		// meta commands that mutate database/session context.
		if handled, err := handleFormatCommand(line, &options, out); handled {
			if err != nil {
				fmt.Fprintln(out, "error:", err)
			}
			fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
			continue
		}

		res, err := executor.Execute(ctx, line)
		if err != nil {
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
		fmt.Fprintln(out, render.RenderStatusLine(executor.Session.StatusLine(), options.Render))
	}
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
