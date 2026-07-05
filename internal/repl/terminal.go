package repl

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"

	"github.com/zhiwangdu/influx-cli/internal/app"
)

type terminalLineReader struct {
	instance *readline.Instance
}

func newTerminalLineReader(ctx context.Context, executor *app.Executor, in io.Reader, out io.Writer) (lineReader, bool, error) {
	inFile, ok := in.(*os.File)
	if !ok || !isTerminalFile(inFile) {
		return nil, false, nil
	}
	outFile, ok := out.(*os.File)
	if !ok || !isTerminalFile(outFile) {
		return nil, false, nil
	}
	instance, err := readline.NewEx(&readline.Config{
		HistoryLimit:           lineHistoryLimit,
		DisableAutoSaveHistory: true,
		Stdin:                  inFile,
		Stdout:                 out,
		Stderr:                 out,
		AutoComplete:           readlineCompleter{ctx: ctx, executor: executor},
		InterruptPrompt:        "^C",
		EOFPrompt:              "^D",
	})
	if err != nil {
		return nil, false, err
	}
	return &terminalLineReader{instance: instance}, true, nil
}

func isTerminalFile(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func (r *terminalLineReader) ReadLine(prompt string) (string, error) {
	r.instance.SetPrompt(prompt)
	line, err := r.instance.Readline()
	if errors.Is(err, readline.ErrInterrupt) {
		return "", errInputInterrupted
	}
	if errors.Is(err, io.EOF) {
		return "", io.EOF
	}
	return line, err
}

func (r *terminalLineReader) Close() error {
	return r.instance.Close()
}

func (r *terminalLineReader) SaveHistory(line string) error {
	return r.instance.SaveHistory(line)
}

type readlineCompleter struct {
	ctx      context.Context
	executor *app.Executor
}

func (c readlineCompleter) Do(line []rune, pos int) ([][]rune, int) {
	ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
	defer cancel()
	completion, err := c.executor.Complete(ctx, string(line), pos)
	if err != nil {
		return nil, 0
	}
	prefixRunes := []rune(completion.Prefix)
	out := make([][]rune, 0, len(completion.Candidates))
	for _, candidate := range completion.Candidates {
		suffix := completionSuffix(candidate, completion.Prefix)
		out = append(out, []rune(suffix))
	}
	return out, len(prefixRunes)
}

func completionSuffix(candidate, prefix string) string {
	candidateRunes := []rune(candidate)
	prefixRunes := []rune(prefix)
	if len(prefixRunes) <= len(candidateRunes) && strings.EqualFold(string(candidateRunes[:len(prefixRunes)]), prefix) {
		return string(candidateRunes[len(prefixRunes):])
	}
	return candidate
}
