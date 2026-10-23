---
type: Task
id: T44
title: "Local rules must survive a network / MQTT outage (evok over loopback, never-give-up startup)"
description: "The bridge talks to evok over 127.0.0.1 so a lost LAN address cannot cut the button->output path; discovery retries forever; the MQTT watchdog no longer restarts the bridge when the network is the problem."
phase: 1
task_status: done
depends_on: [T17]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, robustness, survivability, network]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-23T09:30:00Z }
---

# Objective
Requirement (Matthijs, 2026-10-23): "the local handling (button -> LEDs) must be independent of the network and the MQTT broker, for survivability". After a network outage on 2026-10-22 (the unit also got a new IP) the local rules stopped working.

# Root cause (from the bridge log, 2026-10-23 05:35-07:56)
The config pointed the bridge at evok through the box's **own LAN address** (`ws://<LAN-IP>:8080/ws`, `http://<LAN-IP>:8080/rest/all`). When the interface lost its address the bridge could not reach evok - which runs on the same machine and was fine - for **2 h 20 min** (`Errno 113 No route to host`, `101 Network is unreachable`, retry every 33 s) and every local rule was dead. With a new DHCP address it would have stayed dead until someone edited the config. Evidence: 270 consecutive failed connects to `<LAN-IP>:8080`, then recovery the moment the address was back. (Older log lines had rotated away.)

# Other hazards found while checking the path button -> output
1. No WebSocket message is processed until discovery is complete, and a failed discovery (5 REST tries) was **never retried** -> rules dead until a manual restart.
2. The MQTT watchdog exited the bridge after 600 s without a broker even when the cause was the network/broker -> a ~12 s deaf gap every 10 minutes during a long outage, fixing nothing.
3. paho's offline publish queue is unbounded -> memory grows during a long outage.
4. The HA "Visit" link was derived from the evok URL host; mapping evok to loopback would have turned it into `http://127.0.0.1:8088`.

# Changes (`rc18`)
- `evok_url_to_loopback()`: a `websocket.url` / `unipi_http.url` whose host is this machine (a bindable local address or this hostname) is used as `127.0.0.1` (WARNING in the log). Other hosts untouched. The live config was also edited to `127.0.0.1` (backup `pre-T44-config`), so it holds even if the bridge starts while the address is missing.
- Discovery is retried every 30 s until it succeeds.
- MQTT watchdog: after 600 s down it probes the broker with a TCP connect; unreachable -> stay up (look again in 5 min); reachable (stuck client) -> exit as before.
- `max_queued_messages_set(2000)` on the paho client (everything is republished after reconnecting anyway).
- HA link: `web_server.public_host` if set, else the configured host, else (loopback) the box's current LAN address (UDP connect to the broker, no traffic; `localhost` if no route).
- Example config uses `127.0.0.1`.

# Evidence
- Tests `tests/test_survivability.py` (11): URL mapping (local/other/loopback), HA link, paho cap, discovery retry, monitor stays up when unreachable / restarts a stuck client / probes once per 5 min, broker probe, and an **end-to-end test with the real bridge process, a fake evok and no broker**: evok configured on "the box's own address", button -> output command arrives; evok drops the connection -> bridge reconnects -> rule works again. 8 mutation checks, all caught; with the URL mapping disabled the end-to-end test fails exactly like the real outage ("bridge never connected to evok").
- Test lesson: the first end-to-end run hung because I had given the bridge a stdout PIPE that nobody read (it blocked on the full pipe). Test artefact only - under systemd the journal reads stdout.
- Not tested here: a real interface-down on the S103 (would cut the session); the end-to-end test models it with a local address that no longer routes to the server.

# Open questions
- Should rules also start before discovery completes (using the cached device name)? Today they wait for it; with evok on loopback this is a few seconds after start.
- Shadow/dev instances that point at a *remote* evok keep working unchanged.
