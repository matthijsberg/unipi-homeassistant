# Tasks

Execution order = table order unless `depends_on` allows parallel work. 🔒 = contains human
gates (a goldfish prepares, Matthijs decides/acts). Update `task_status` here **and** in the card.

# Phase 0 — Safety net (no behaviour change)

* [T00 Restore GitHub push access](T00-github-access.md) - 🔒 · low · deps: – · **done 2026-10-07**
* [T01 Baseline backups + what really runs on the L513](T01-baseline-backups.md) - 🔒 · low · deps: – · **in progress** (off-box copy pending)
* [T02 Import live bridge as v2.0.0 + secret guard](T02-bootstrap-repo.md) - low · deps: T00, T01 · todo
* [T03 Backup/deploy/rollback/health tooling](T03-deploy-rollback-tooling.md) - medium · deps: T02 · todo
* [T04 Test harness + characterization tests](T04-test-harness.md) - low · deps: T02 · todo

Milestone **v2.1.0** = T03 + T04 merged (runtime code identical to v2.0.0).

# Phase 1 — Generic core features (developed and soaked on the S103)

* [T10 EventBus + CommandService (refactor)](T10-core-events-commands.md) - medium · deps: T04 · todo
* [T11 Per-circuit config, names, logical inversion](T11-circuit-config.md) - medium · deps: T10 · todo
* [T12 Output sequencer: pulse / duration / safety](T12-output-sequencer.md) - **high** · deps: T11 · todo
* [T13 Presets as HA buttons](T13-ha-presets-buttons.md) - low · deps: T12 · todo
* [T14 PIR hold (off-delay)](T14-input-hold-off-delay.md) - medium · deps: T11 · todo
* [T15 Counter inputs](T15-counter-inputs.md) - low · deps: T11 · todo
* [T16 Sensor transform / sampling / validation](T16-sensor-transform-sampling.md) - medium · deps: T11 · todo
* [T17 Rule actions pulse / toggle / dimmer level](T17-local-rule-actions.md) - medium · deps: T12 · todo
* [T18 Shadow mode](T18-shadow-mode.md) - 🔒 · low · deps: T10 · todo
* [T19 (optional) AO as fan/number](T19-ao-component-override.md) - 🔒 · low · deps: T11 · todo

Milestone **v2.2.0** = T10–T18 merged, S103 healthy 7 days on the last rc.
T14/T15/T16/T18 can run in parallel after T11 (different files, except small hooks in
`hass-unipi.py` — merge one at a time and rebase).

# Phase 2 — Legacy adapter (L513 still untouched)

* [T20 Capture the complete legacy HA contract](T20-capture-legacy-contract.md) - 🔒 · low · deps: T01 · todo (can start any time after T01)
* [T21 legacy_adapter.py behind legacy.enabled](T21-legacy-adapter.md) - **high** · deps: T12, T14–T17, T20 · todo
* [T22 Old-config converter](T22-legacy-config-converter.md) - medium · deps: T21 · todo
* [T23 End-to-end validation + v2.3.0](T23-legacy-validation.md) - 🔒 · medium · deps: T18, T21, T22 · todo

# Phase 3 — L513 upgrade and cutover (maintenance window)

* [T30 Pre-flight + go/no-go](T30-l513-preflight.md) - 🔒 · medium · deps: T01, T23 · todo
* [T31 Flash evok 3 on a new card + circuit map](T31-l513-flash-and-map.md) - 🔒 · **high** · deps: T30 · todo
* [T32 Start bridge with legacy adapter + acceptance walk (v2.4.0)](T32-l513-cutover.md) - 🔒 · **high** · deps: T31, T22 · todo
* [T33 7-day soak + accept](T33-l513-soak.md) - 🔒 · medium · deps: T32 · todo

# Phase 4 — Home Assistant migration and legacy removal

* [T40 HA to discovered entities, per area](T40-ha-migration.md) - 🔒 · medium · deps: T33 · todo
* [T41 Adapter off 14 days + cleanup](T41-disable-legacy.md) - 🔒 · low · deps: T40 · todo
* [T42 Remove adapter, release v3.0.0](T42-remove-legacy-release-v3.md) - 🔒 · low · deps: T41 · todo
