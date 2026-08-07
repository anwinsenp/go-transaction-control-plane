---
description: Run the test-writer subagent against a package or file
---

Use the `test-writer` subagent to add/expand table-driven tests for
$ARGUMENTS, following the repo's existing testing conventions. The subagent
already runs `go test -race` scoped to the package(s) it touched and
reports the result — don't re-run the full `./...` suite here on top of it.
