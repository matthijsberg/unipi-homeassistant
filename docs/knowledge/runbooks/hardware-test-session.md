---
type: Runbook
title: "Hardware test session: what only a person at the Unipi can verify"
description: "Checklist of the live tests that software cannot do alone (editor click-through, real buttons, HA outage, physical timing, counters, shadow run), with exactly what to do and what 'pass' looks like."
tags: [runbook, testing, manual]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-07T18:00:00Z }
---

State when written: S103 runs `v2.2.0-rc7` (T10–T12, T15, T17, T18 live; T13 not built; T14/T16 dropped to Home Assistant).
House impact of every step below is stated; none switches anything but the front-panel LED unless you say so.

# 0. Reading the new "Rule activity" panel (rc8)
Right-hand side of the editor, below Live Diagnostics. Each line = one step the bridge took for a rule: ▶️ trigger matched, ○ trigger seen but
the value did not match (says what it needs), ⛔ a condition stopped it (says which), ⏭ skipped because Home Assistant is reachable, ⏳ delayed,
✅ executed (says what was sent), ⚠️ disabled/refused/error (says why). The rule's block flashes blue/green/orange. A rule with a problem shows a
warning icon on its block. **Use this panel in every step below: if nothing happens, it tells you why.**

# 1. Rule editor click-through (T17) — 5 min, no hardware effect
Open `http://<S103_IP>:8088`, log in. Pass when:
1. Existing rules (none today) load without error.
2. Add a **Pulse output** rule (trigger: any input you don't use, e.g. `di 1_01 eq 1`, action pulse `led 1_01`, preset `blink3`), a **Toggle output** rule and a **Dimmer** rule with level 5 V and "hold" unticked. In each rule set "Runs: only when Home Assistant is unreachable". Save.
3. Reload the page: every rule, field and the "Runs" setting is back; saving again changes nothing.
4. Delete the test rules and save (leave the file as `[]`).

# 1b. Push-to-dim (T17, rc9) — house impact: the lamp on the chosen analog output
Block "Push-to-dim light" (circuit = the analog output of your lamp, switch-on level e.g. 8 V, hold time 800 ms). Pass: a tap switches the lamp on/off;
holding longer than 0.8 s dims it up (lamp on) and keeps going until you let go; the next hold goes the other way; dimming down never switches
the lamp off (it stops at the lowest level); a tap afterwards switches off and the next tap returns to the last level. HA's brightness follows.

# 2. A real button through a local rule (T17) — house impact: the LED only
Put a rule `di <your free input> eq 1 → pulse led 1_01 preset blink3`, `Runs: always`. Press the input: **the front LED blinks three times**
(100 ms on / 250 ms off). Also check the bridge's attribute topic `unipi/<device>/led/1_01/attributes` shows `busy` then `busy:false`.

# 3. Home Assistant outage (T17 `ha_offline`) — house impact: HA unavailable for a minute
Rule with `Runs: only when Home Assistant is unreachable`. With HA **up**, press the input → nothing happens locally. Stop HA (or disable its MQTT
connection) for a minute → press again → the rule acts. Start HA → press → nothing locally again. (MQTT broker stays up.)

# 4. Physical ring timing (T12) — needs a safe `do`/`ro` output, or just your eyes
Evok neither pushes LED changes nor refreshes them quickly, so software cannot time the LED. Either name an S103 output that is safe to
pulse (tell the goldfish which circuit) or watch the front LED while `{"pulse":{"count":3,"on_ms":100,"off_ms":250}}` is sent to
`unipi/<device>/led/1_01/set`. Pass: three crisp blinks, evenly spaced.

# 5. Water-meter style counter (T15) — no house impact
Pick an input with a running counter (several exist). Add `circuits."di/<c>" = {"counter": true, "unit": "L"}` to the live `config.json`
(backup first), restart. Pass: a new sensor "di <c> counter" in HA (`total_increasing`); its value follows evok's counter; with the input quiet no
messages flow; remove the entry afterwards if unwanted.

# 6. Shadow run (T18) — house impact: none by design, 1 h
Follow `/runbooks/shadow-instance.md` with the current tag. Pass: live service unaffected (`tools/healthcheck.py` OK), shadow status
`online`, `tools/shadow_compare.py --seconds 60` shows no systematic state differences, a command sent to a *shadow* topic only produces a
`SHADOW: would …` log line (the real output does not move), and `grep -c "SHADOW: would write" ~/.local/logs/unipi_shadow.log` > 0 while the
evok output stays unchanged. Then clean up with `tools/clear_shadow_topics.py --yes`.

# 7. Decisions waiting for Matthijs
- T21 (legacy adapter): PIR hold and lux scaling now live in HA — migrate those HA entities before the L513 cutover, or let the adapter reimplement them?
- Confirm the L513 boots from a removable µSD (T30).
- The flaky 1-wire sensor `…0063` (missing from evok since the reboot): check wiring or retire its HA entities.
