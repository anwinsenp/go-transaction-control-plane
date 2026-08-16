---
description: Plan an implementation, and on approval build it then auto-run test-writer + code-reviewer in parallel
---

For $ARGUMENTS:

1. If `$ARGUMENTS` is a bare issue number (or `#<number>`), run
   `gh issue view <number>` first and use its title/body/acceptance
   criteria as the actual spec to plan against, instead of treating the
   number itself as the task description. Otherwise treat `$ARGUMENTS` as a
   plain-text task description directly.
2. Restate the problem/constraints in 2-3 sentences, list any assumptions,
   and propose the key types/function signatures and the build order
   (same shape as `/plan`). If working from an issue, make sure the plan
   covers every acceptance criterion.
3. Unlike `/plan`, wait for explicit approval before writing any code —
   don't proceed automatically.
4. Once approved, implement the change, scoped to the package(s) the task
   actually touches.
5. In a single message, dispatch two subagents in parallel — do not chain
   them, they don't depend on each other's output:
   - `test-writer`, scoped to the package(s) just changed.
   - `code-reviewer`, scoped to the same diff (`git diff`).
6. Once `test-writer` and `code-reviewer` finish, run `golangci-lint run
   ./...` (scoped to the touched package(s) is fine if the full run is slow)
   over the combined diff, including the new test file.
7. Report a combined summary: tests added/coverage delta from
   `test-writer`; review findings tagged **[blocking]**/**[nit]** from
   `code-reviewer`; and any `golangci-lint` findings. If anything is
   blocking, a test fails, or lint fails, propose the fix (use the
   `debugger` subagent if it's a real bug rather than a missing test or
   lint nit) and wait for approval before applying it.
8. Stop here. This command doesn't run the full-repo gate or commit — once
   everything is clean, use `/ship <issue>` or `/commit` next.
