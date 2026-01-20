# Contributing to VisioEval

## Getting Started

1. Fork the repository
2. Clone your fork
3. Set up the development environment (see [DEVELOPMENT.md](DEVELOPMENT.md))
4. Create a feature branch

## Branch Naming

Use descriptive prefixes:

| Prefix | Purpose |
|--------|---------|
| `feature/` | New functionality |
| `fix/` | Bug fixes |
| `docs/` | Documentation only |
| `refactor/` | Code restructuring |
| `test/` | Test additions/fixes |

Example: `feature/add-outlier-analyzer`

## Commit Messages

Write clear, descriptive commit messages:

```
Add outlier detection analyzer

- Implement z-score based outlier detection
- Add AnalysisKind.OUTLIERS_1D flag
- Register in ANALYSIS_PIPELINE
```

**Format:**
- First line: imperative mood, max 72 chars
- Blank line
- Body: explain what and why (not how)

## Pull Request Process

1. **Before submitting:**
   - Run `pixi run lint` - no errors
   - Run `pixi run typecheck` - no errors
   - Run `pixi run test` - all pass
   - Run `pixi run precommit-run` - all pass

2. **PR description should include:**
   - What the change does
   - Why it's needed
   - How to test it
   - Any breaking changes

3. **Review process:**
   - All CI checks must pass
   - At least one maintainer approval
   - Address all review comments

## Code Review Checklist

When reviewing, check for:

- [ ] Code follows [code standards](.claude/docs/code_standards.md)
- [ ] Tests cover new functionality
- [ ] No security issues (see [security guidelines](.claude/docs/security.md))
- [ ] Documentation updated if needed
- [ ] No unnecessary dependencies added

## Good Contribution Areas

### Algorithms

New algorithm implementations. See [extending guide](.claude/docs/extending.md).

### Analyzers

New analysis types for metrics. Follow the Analyzer protocol.

### Testing

Additional test coverage, especially edge cases.

### Documentation

Improvements to docs, examples, tutorials.

### Performance

Profiling, optimization, benchmarks.

## What We Don't Accept

- Breaking changes without discussion
- Large refactors without prior approval
- Dependencies without justification
- Code without tests
- Commits with secrets or credentials

## Questions?

Open an issue with the `question` label.

## Code of Conduct

Be respectful. We're all here to build something useful.
