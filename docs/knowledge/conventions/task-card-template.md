---
type: Template
title: "Task card template"
description: "Copy this file to /tasks/Txx-name.md when adding a new task; every section is mandatory (write \"none\" if empty)."
tags: [process, template]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

```markdown
---
type: Task
id: Txx
title: <imperative title>
description: <one sentence: what is true when this task is done>
phase: <0-4>
task_status: todo
depends_on: [Tyy]
risk: low | medium | high
human_gate: false
target_hosts: [dev-only | s103 | l513]
tags: []
status: draft
generated: { by: <actor>, at: <ISO time> }
---

# Objective
Why this task exists, in 2–4 sentences. Link the gap row / ADR it serves.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- <links to the concepts needed, nothing more>

# Preconditions
Commands + expected result. Mismatch ⇒ STOP (task_status: blocked).

# Files in scope
Exact paths that may be created/changed. Nothing else.

# Backup
Exact command(s).

# Steps
1. …

# Acceptance checks
Commands/observations + expected result. All must pass.

# Rollback
How to undo this task on every host it touched.

# Feed the elephant
Which concepts/log entries to update.

# Evidence
(filled in by the executor)

# Open questions
(filled in by the executor)
```
