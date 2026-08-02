---
description: Verify current changes against a GitHub issue's acceptance criteria, commit, and close/comment on the issue
---

Use the `issue-closer` subagent for issue $ARGUMENTS (the issue number).
Check the current diff against its acceptance criteria and the standard
gofmt/vet/build/test checks, report the checklist, and only proceed to
commit and update the issue after I confirm.
