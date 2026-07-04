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
