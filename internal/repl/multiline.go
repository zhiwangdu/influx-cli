package repl

import (
	"strings"
	"unicode"
)

type statementBuffer struct {
	lines  []string
	active bool
}

func (b *statementBuffer) Active() bool {
	return b.active
}

func (b *statementBuffer) Reset() {
	b.lines = nil
	b.active = false
}

func (b *statementBuffer) Add(line string) (string, bool) {
	line, explicitContinue := stripTrailingContinuation(line)
	line = strings.TrimRightFunc(line, unicode.IsSpace)
	b.lines = append(b.lines, line)
	statement := strings.Join(b.lines, "\n")
	state := analyzeStatement(statement)

	if b.active {
		if state.Terminated && !state.Incomplete() {
			b.Reset()
			return statement, true
		}
		return "", false
	}

	if state.Terminated && !state.Incomplete() {
		b.Reset()
		return statement, true
	}

	if explicitContinue || state.Incomplete() || looksLikeIncompleteSelect(statement) || endsWithContinuationToken(statement) {
		b.active = true
		return "", false
	}

	b.Reset()
	return statement, true
}

func normalizeStatement(statement string) string {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return ""
	}
	if trimmed, ok := trimFinalSemicolon(statement); ok {
		return strings.TrimSpace(trimmed)
	}
	return statement
}

func stripTrailingContinuation(line string) (string, bool) {
	trimmed := strings.TrimRightFunc(line, unicode.IsSpace)
	if !strings.HasSuffix(trimmed, `\`) {
		return line, false
	}
	return strings.TrimRightFunc(strings.TrimSuffix(trimmed, `\`), unicode.IsSpace), true
}

func trimFinalSemicolon(statement string) (string, bool) {
	state := lexState{}
	lastSemicolon := -1
	for index, r := range statement {
		state.Step(statement, index, r)
		if state.TopLevel() && r == ';' {
			lastSemicolon = index
		}
	}
	if lastSemicolon < 0 {
		return statement, false
	}
	if strings.TrimSpace(statement[lastSemicolon+1:]) != "" {
		return statement, false
	}
	return statement[:lastSemicolon], true
}

type statementState struct {
	Terminated   bool
	SingleQuote  bool
	DoubleQuote  bool
	Backtick     bool
	Regex        bool
	BlockComment bool
	ParenDepth   int
	BracketDepth int
	BraceDepth   int
}

func (s statementState) Incomplete() bool {
	return s.SingleQuote ||
		s.DoubleQuote ||
		s.Backtick ||
		s.Regex ||
		s.BlockComment ||
		s.ParenDepth > 0 ||
		s.BracketDepth > 0 ||
		s.BraceDepth > 0
}

func analyzeStatement(statement string) statementState {
	lex := lexState{}
	result := statementState{}
	for index, r := range statement {
		lex.Step(statement, index, r)
		result.SingleQuote = lex.singleQuote
		result.DoubleQuote = lex.doubleQuote
		result.Backtick = lex.backtick
		result.Regex = lex.regex
		result.BlockComment = lex.blockComment
		result.ParenDepth = lex.parenDepth
		result.BracketDepth = lex.bracketDepth
		result.BraceDepth = lex.braceDepth
		if lex.TopLevel() && r == ';' && strings.TrimSpace(statement[index+1:]) == "" {
			result.Terminated = true
		}
	}
	return result
}

type lexState struct {
	singleQuote  bool
	doubleQuote  bool
	backtick     bool
	regex        bool
	blockComment bool
	lineComment  bool
	escaped      bool
	parenDepth   int
	bracketDepth int
	braceDepth   int
}

func (s *lexState) TopLevel() bool {
	return !s.singleQuote && !s.doubleQuote && !s.backtick && !s.regex && !s.blockComment && !s.lineComment
}

func (s *lexState) Step(statement string, index int, r rune) {
	next := byte(0)
	if index+1 < len(statement) {
		next = statement[index+1]
	}

	if s.lineComment {
		if r == '\n' {
			s.lineComment = false
		}
		return
	}
	if s.blockComment {
		if r == '*' && next == '/' {
			s.blockComment = false
		}
		return
	}
	if s.singleQuote {
		if s.escaped {
			s.escaped = false
			return
		}
		if r == '\\' {
			s.escaped = true
			return
		}
		if r == '\'' {
			s.singleQuote = false
		}
		return
	}
	if s.doubleQuote {
		if s.escaped {
			s.escaped = false
			return
		}
		if r == '\\' {
			s.escaped = true
			return
		}
		if r == '"' {
			s.doubleQuote = false
		}
		return
	}
	if s.backtick {
		if r == '`' {
			s.backtick = false
		}
		return
	}
	if s.regex {
		if s.escaped {
			s.escaped = false
			return
		}
		if r == '\\' {
			s.escaped = true
			return
		}
		if r == '/' {
			s.regex = false
		}
		return
	}

	switch {
	case r == '-' && next == '-':
		s.lineComment = true
	case r == '/' && next == '/':
		s.lineComment = true
	case r == '/' && next == '*':
		s.blockComment = true
	case r == '\'':
		s.singleQuote = true
	case r == '"':
		s.doubleQuote = true
	case r == '`':
		s.backtick = true
	case r == '/' && previousNonSpace(statement, index) == '~':
		s.regex = true
	case r == '(':
		s.parenDepth++
	case r == ')' && s.parenDepth > 0:
		s.parenDepth--
	case r == '[':
		s.bracketDepth++
	case r == ']' && s.bracketDepth > 0:
		s.bracketDepth--
	case r == '{':
		s.braceDepth++
	case r == '}' && s.braceDepth > 0:
		s.braceDepth--
	}
}

func previousNonSpace(statement string, index int) rune {
	for i := index - 1; i >= 0; i-- {
		r := rune(statement[i])
		if !unicode.IsSpace(r) {
			return r
		}
	}
	return 0
}

func looksLikeIncompleteSelect(statement string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(statement))
	if !strings.HasPrefix(trimmed, "select ") {
		return false
	}
	return !containsWord(trimmed, "from")
}

func endsWithContinuationToken(statement string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(statement))
	if trimmed == "" {
		return false
	}
	for _, suffix := range []string{"|>", ",", "+", "-", "*", "/", "%", "=", "<", ">", "and", "or"} {
		if strings.HasSuffix(trimmed, suffix) {
			return true
		}
	}
	return false
}

func containsWord(input, word string) bool {
	fields := strings.FieldsFunc(input, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, field := range fields {
		if field == word {
			return true
		}
	}
	return false
}
