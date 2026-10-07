---
okf_version: "0.2"
title: "Unipi ⇄ Home Assistant bridge — knowledge bundle"
description: "The project's long-term memory (the \"Elephant\"): context, contracts, decisions, runbooks and self-contained task cards for migrating the L513 from unipi_mqtt.py to hass-unipi.py."
---

# Start here (every AI model, every session)

* [Elephant & Goldfish protocol](conventions/elephant-goldfish.md) - **mandatory** working rules: read this first, always
* [Task list](tasks/) - pick the first `todo` task whose dependencies are `done`
* [Log](log.md) - what happened, newest first

# Context

* [System overview](context/system-overview.md) - goal, target architecture, principles
* [Hardware inventory](context/hardware-inventory.md) - both Unipis, evok versions, network
* [Legacy MQTT contract](context/interface-legacy.md) - what HA exchanges with the old script today
* [Core MQTT contract](context/interface-core.md) - current + planned topics, pulse/duration command, per-circuit config
* [Code map](context/code-map.md) - where things are in hass-unipi.py and where new code goes

# Analysis

* [Feature gap matrix](analysis/feature-gap-matrix.md) - every old feature → core / HA / adapter / drop
* [Known issues](analysis/known-issues.md) - bugs and risks found in review

# Decisions

* [ADR-001 Upgrade L513 to evok 3](decisions/ADR-001-upgrade-l513-to-evok3.md) - no evok-v2 support in code
* [ADR-002 Removable legacy adapter](decisions/ADR-002-legacy-adapter.md) - one switch, translation only
* [ADR-003 Pulse/duration sequencer](decisions/ADR-003-pulse-sequence-command.md) - doorbell in one MQTT message, with safety
* [ADR-004 Versioning and backups](decisions/ADR-004-versioning-and-backups.md) - GitHub, semver tags, backups
* [ADR-005 Core vs Home Assistant](decisions/ADR-005-core-vs-home-assistant.md) - where logic lives
* [ADR-006 BaseOS deployment](decisions/ADR-006-baseos-deployment.md) - bootstrap script, release folders, site bundle

# Runbooks

* [Backup and rollback](runbooks/backup-and-rollback.md) - before every change; how to go back
* [Deploy](runbooks/deploy.md) - the only way to change a live box
* [Shadow instance](runbooks/shadow-instance.md) - test a new version next to the live bridge, risk-free
* [Hardware test session](runbooks/hardware-test-session.md) - what only a person at the Unipi can verify, step by step

# Conventions

* [Task card template](conventions/task-card-template.md) - for new tasks
