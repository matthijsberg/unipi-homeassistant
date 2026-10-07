---
type: Playbook
title: "Elephant & Goldfish working protocol"
description: "How any AI model (or human) executes work in this project without relying on memory, while the project itself never forgets."
tags: [process, ai-agents, mandatory]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Why

This project controls a **live house** (doorbell, lights, roof window motor, heat-pump
smart-grid relays, leak sensors). Work will be handed between different AI models and
humans over weeks. Chat history is lost; files are not. So we split responsibilities:

| Role | Metaphor | What it is | Rule |
|---|---|---|---|
| **Elephant** | never forgets | This OKF bundle (`docs/knowledge/`) + git history + backups | Every fact, decision and result that matters is written here. If it is not in the elephant, it did not happen. |
| **Goldfish** | remembers nothing | The executor of one task (any AI model, any session) | Assumes zero prior knowledge. Reads only this playbook, its task card and the concepts the card links. Verifies the world before acting. |

# Goldfish protocol (mandatory for every task)

1. **Load context** – read, in order: this file → `/tasks/<your task>.md` → every concept
   linked under *Read first* in the card. Read nothing else unless the card says so.
2. **Check preconditions** – run every command in *Preconditions*. If any result differs
   from what the card expects: **STOP**. Set `task_status: blocked`, write why in the card's
   *Evidence* section and in `/log.md`. Do not "fix the world" to make it match.
3. **Backup first** – run the card's *Backup* step (normally `tools/backup.sh <label>`, or the
   manual equivalent from `/runbooks/backup-and-rollback.md` before T03 exists).
   No backup ⇒ no change.
4. **Stay in scope** – only touch files listed under *Files in scope*. Anything else you think
   needs changing → write it as an *Open question* in the card; do not do it.
5. **Do the steps** – in order. Steps marked **🔒 HUMAN** must be done by Matthijs; the
   goldfish prepares everything, then stops and asks.
6. **Prove it** – run every *Acceptance check*. Paste the real output (trimmed) into the
   card's *Evidence* section. A check that was not run is a failed check.
7. **Feed the elephant** – update: the card (`task_status`, Evidence), `/log.md` (newest
   first), any concept whose facts changed (bump its `generated`), and `/tasks/index.md`.
8. **Commit** – one task = one branch `task/Txx-short-name` = one PR. Commit message:
   `Txx: <summary>` + body listing acceptance results. Never commit secrets (the pre-commit
   hook from T02 blocks known ones). Never push tags or deploy to a live box unless the card
   says so **and** the human approved it in this session.
9. **When unsure** – ask; don't guess. Record the question and the answer in the card.

# Elephant rules

- **Single source of truth.** Architecture, contracts, inventory and decisions live in
  `/context/`, `/decisions/`, `/analysis/`. Code comments may summarise, never contradict.
- **Decisions are ADRs** (`type: Decision`). Changing a decision = new ADR that
  supersedes the old one; set the old one to `status: deprecated`. Never silently edit.
- **Nothing is deleted.** Obsolete concepts get `status: deprecated` + a pointer to the
  replacement.
- **Trust tiers (OKF §5.2).** Content written by an AI has `generated:` only (= *unverified*).
  After Matthijs has reviewed a concept he (or the goldfish on his explicit instruction) adds
  `verified: { by: "human:matthijs", at: <ISO time> }`. Tasks marked `human_gate: true`
  may only start when every concept in their *Read first* list is human-reviewed.
- **Public bundle, private values.** The repo is public. The bundle uses placeholders
  (`<S103_IP>`, `<L513_IP>`, `<BROKER_IP>`, `<MQTT_USER_S103>`). The real values are in the
  gitignored `docs/knowledge/local/environment.md` on each box — read it when you need to run
  a command. **Never write a real IP, hostname, username, e-mail or password into a tracked
  file**; use the placeholder. The pre-commit and pre-push hooks (`tools/pii_scan.py`) block
  violations; do not bypass them (`--no-verify` is forbidden; pre-push re-checks anyway).
  A false positive is handled by fixing the scanner or adding `pii-ok` to that line, with a
  reason in the commit message.
- **Facts carry evidence.** Inventory facts cite how they were obtained (command + date) via
  `sources:`.

# Task card format

See `/conventions/task-card-template.md`. Task frontmatter uses OKF `type: Task` plus
project keys (OKF consumers ignore unknown keys): `id`, `phase`, `task_status`
(`todo | in_progress | blocked | done`), `depends_on`, `risk` (`low | medium | high`),
`human_gate` (bool), `target_hosts`.

# Sizing rules

- One concern per task; aim for < 400 changed lines and < 1 day.
- Every task leaves both Unipis in a working state. No "half-done until next task".
- Every behaviour change ships behind a config option whose default keeps today's behaviour
  on the S103 (`<S103_IP>`), unless the card explicitly says otherwise.
