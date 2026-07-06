# AGENTS.md

Instructions for coding agents and contributors working in this repository.

## Source Of Truth

Use these documents as the primary reference, in this order:

1. `docs/PRODUCT_DESIGN.md`
2. `docs/ARCHITECTURE.md`
3. `docs/ROADMAP.md`
4. `README.md`

Legacy drafts under `docs/legacy/` are historical material only. Do not use them for design, architecture, roadmap, naming, or implementation decisions unless the user explicitly asks for legacy context.

## Project State

The repository is currently documentation-first. There is no Go implementation yet.

Expected next work is Phase 0 from `docs/ROADMAP.md`:

1. Create the Go module and CLI entrypoint.
2. Implement config/profile loading.
3. Implement the InfluxDB 1.x HTTP query adapter.
4. Normalize query results into table and series models.
5. Implement table and sparkline renderers.
6. Add `query` and `repl` commands.

## Product Constraints

Keep the MVP narrow:

- Query execution.
- REPL.
- Table rendering.
- Sparkline rendering.
- Session/statusline context.

Do not add these in the MVP unless explicitly requested:

- Full Bubble Tea dashboard.
- Plugin system.
- Storage file parser.
- Query optimizer.
- Complete Flux parser.
- Multi-panel dashboard.

## Architecture Constraints

Follow `docs/ARCHITECTURE.md`.

Important boundaries:

- UI must not call database clients directly.
- CLI, REPL, TUI, and watch mode must share query orchestration.
- Adapters return normalized `Result` models, not UI-specific output.
- Renderers consume `Result` models and must not contain adapter logic.
- Analyzer/profiler code must stay optional and must not block the basic query loop.
- openGemini support should initially reuse the InfluxDB-compatible HTTP query path where possible.

## Coding Guidance

When implementation starts:

- Prefer small, testable packages under `internal/`.
- Keep public API surface minimal until the MVP is stable.
- Add unit tests for config merge, result normalization, and renderers.
- Use fake adapters for CLI/REPL tests before requiring a live database.
- Keep errors explicit and user-readable. Do not print secrets.
- All long-running query paths should accept `context.Context`.

## Local Claude Review Rule

For every coding implementation:

- Before each review round, first probe that `claude` is responsive. For example:
  `claude -p "Health check: reply with exactly OK and no other text." --no-session-persistence --permission-mode dontAsk --output-format text`
- Treat the probe as successful only when it exits successfully and its output is exactly `OK` after trimming surrounding whitespace; otherwise treat `claude` as unavailable for that review round. Use a short probe timeout, such as 60 seconds, so a hung probe does not consume the review budget.
- Run the local `claude` command for iterative code review before committing.
- Treat one review round as one `claude` review command, and allow up to 30 minutes for each review command before treating it as timed out. Enforce this with the command runner's timeout or polling budget, or with an equivalent shell timeout mechanism where available.
- Review Claude's findings yourself, then apply appropriate fixes.
- Repeat the review/fix loop as needed, but stop after at most 5 review rounds for a single implementation to avoid non-converging review loops.
- If `claude` is unavailable or cannot complete, state that in the final response and proceed with the best available verification.

## Commit And Push Rule

After each completed modification and verification step, commit and push the changes before starting unrelated work.

Commit messages must clearly describe the concrete change points. Prefer concise conventional-style messages, for example:

- `docs: organize product design and roadmap`
- `feat: add influx query adapter`
- `test: cover sparkline renderer edge cases`

Do not batch unrelated work into one commit. If verification cannot be run, state that in the final response and still keep the commit message specific to the completed change.

## Documentation Guidance

Update formal docs when product or architecture decisions change.

If a legacy draft conflicts with a formal document, the formal document wins.

Do not expand README into a full design document. Keep it as the project entrypoint and link to `docs/`.
