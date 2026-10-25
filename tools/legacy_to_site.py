#!/usr/bin/env python3
"""Turn the legacy L513 configuration (unipi_mqtt_config.json) into what the new bridge needs - and tell Home Assistant what changes.

  legacy_to_site.py --legacy unipi_mqtt_config.json [--ha-entities ha_entities.json] [--evok2-rest l513_evok2_rest_all.json]
                    [--uart-prefix xS30] [--device-name Neuron_L513_10] --out-dir site/l513 [--map-doc docs/.../l513-migration-map.md]

Writes
  circuits.json     the "circuits" object for config.json (names, device classes, inversion, counter, doorbell/window limits)
  local_rules.json  every legacy `handle_local` (doorbells, light switches) as a local rule of the new bridge
  (--map-doc)       the old -> new name map for Home Assistant and the list of what now has to be done in Home Assistant

Facts it relies on (all taken from the legacy script / evok-2 snapshot, see docs/knowledge/context/ha-legacy-entities.md):
  * evok-2 `relay` and `output` are the same hardware on the L513 (no digital outputs exist) -> both become `ro`
  * `device_normal: nc` means the contact is closed in rest: the new `inverted: true` gives the same ON/OFF
  * legacy local bell: relay on 100 ms, off 300 ms, `rings` times; legacy dimmer = toggle between 0 V and `level` volts
  * UART_4_4_NN (the xS30 extension) becomes <uart-prefix>_NN: ASSUMPTION until the evok-3 listing of the real card is known
    (T31) - `--evok3-rest` checks every generated circuit against that listing.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

BELL_ON_MS, BELL_OFF_MS = 100, 300          # legacy unipython.ring_bel: 0.1 s on, 0.3 s off
DEV_MAP = {"input": "di", "relay": "ro", "output": "ro", "analogoutput": "ao", "ai": "ai"}


class Converter:
    def __init__(self, legacy: list[dict], ha: dict | None = None, evok2: list[dict] | None = None, uart_prefix: str = "xS30",
                 device_name: str = "Neuron_L513_10"):
        self.legacy, self.ha, self.evok2 = legacy, ha or {}, evok2 or []
        self.prefix, self.device = uart_prefix, device_name
        self.notes: list[str] = []
        self._timed: dict[str, int] = {}        # "ro/1_01" -> longest duration in seconds (outputs HA drives with a `duration`)
        self._onewire = {x["circuit"] for x in self.evok2 if x.get("dev") == "1wdevice"}
        self._ha_by_topic = {}
        for plat, lst in self.ha.items():
            for e in lst:
                for key in ("state_topic",):
                    if e.get(key):
                        self._ha_by_topic.setdefault(e[key], (plat, e))

    # ---- naming ------------------------------------------------------------------------------------------------------
    def circuit(self, c: str) -> str:
        m = re.fullmatch(r"UART_\d+_\d+_(\d+)", c)
        return f"{self.prefix}_{m.group(1)}" if m else c

    def key(self, dev: str, c: str) -> str:
        if dev == "temp":
            return f"1wdevice/{c}/temp" if c in self._onewire else f"temp/{c}"
        if dev == "humidity":
            return f"1wdevice/{c}/humidity"
        return f"{DEV_MAP[dev]}/{self.circuit(c)}"

    def ha_for(self, topic: str):
        return self._ha_by_topic.get(topic, (None, None))

    # ---- circuits ------------------------------------------------------------------------------------------------------
    def display_name(self, e: dict, plat: str | None, ha: dict | None) -> str:
        """The name the new HA entity gets. Where the old HA entity is a plain mirror of the signal (contact, lock, leak sensor,
        bell button, temperature ...) the SAME name is used, so that - once the old entity is removed - HA hands out the same
        entity_id and automations keep working. Signals HA must now post-process keep a 'raw' name, because the old name
        belongs to the template sensor the user will create (PIR with delay, lux from volts, water meter in litres)."""
        desc = e["description"]
        if e["dev"] == "ai":
            return f"{desc} (V)"
        if e.get("device_type") == "counter":
            return f"{desc} ticks"
        if e["dev"] == "input" and e.get("device_delay"):
            return desc                                           # PIR: raw input; the old name goes to the template sensor
        if ha and plat in ("binary_sensor", "sensor") and ha.get("name"):
            return ha["name"].strip()
        return desc

    def circuits(self) -> dict:
        out: dict[str, dict] = {}
        for e in self.legacy:
            dev, c = e["dev"], e["circuit"]
            plat, ha = self.ha_for(e.get("state_topic", ""))
            cfg: dict = {"name": self.display_name(e, plat, ha)}
            if dev == "input":
                if ha and plat == "binary_sensor" and ha.get("device_class"):
                    cfg["device_class"] = ha["device_class"]
                if e.get("device_normal") == "nc":
                    cfg["inverted"] = True
                if e.get("device_type") == "counter":
                    cfg.update({"counter": True, "counter_interval_s": max(1, float(e.get("device_delay") or 10)), "unit": "ticks"})
            out[self.key(dev, c)] = cfg
        out.update(self._output_circuits(out))
        return dict(sorted(out.items()))

    def _output_circuits(self, existing: dict) -> dict:
        """Outputs are not in the legacy JSON: they come from the local-handler targets and from what Home Assistant commands."""
        outs: dict[str, dict] = {}
        targets = [(e["handle_local"]["output_dev"], e["handle_local"]["output_circuit"]) for e in self.legacy if "handle_local" in e]
        ha_cmds = self._ha_commanded_outputs()
        for (dev, c), name in ha_cmds.items():
            outs[f"{DEV_MAP.get(dev, dev)}/{c}"] = {"name": name}
        for dev, c in targets:
            outs.setdefault(f"{DEV_MAP[dev]}/{c}", {"name": f"{dev} {c}"})
        # doorbell relay: the coil must never stay on (legacy comment: "my bel will burn out")
        bell_keys = {f"{DEV_MAP[e['handle_local']['output_dev']]}/{e['handle_local']['output_circuit']}" for e in self.legacy
                     if e.get("handle_local", {}).get("type") == "bel"}
        rings = sorted({int(e["handle_local"]["rings"]) for e in self.legacy if e.get("handle_local", {}).get("type") == "bel"})
        for k in bell_keys:
            outs.setdefault(k, {})
            outs[k]["name"] = "Bel"
            outs[k].update({"failsafe_off": True, "max_on_s": 5, "max_count": max(6, *(rings or [0])),
                            "pulse_defaults": {"on_ms": BELL_ON_MS, "off_ms": BELL_OFF_MS},
                            "presets": {f"ring_{n}x": {"label": f"{n}x bellen", "pulse": {"count": n, "on_ms": BELL_ON_MS, "off_ms": BELL_OFF_MS}} for n in rings}})
        # motor relays that HA drives with a `duration` (the roof window, 25 s / 35 s): never left on after a crash,
        # and a hard upper bound a bit above the longest run
        for k, seconds in self._timed.items():
            if k in outs:
                outs[k].update({"failsafe_off": True, "max_on_s": seconds + 5})
        return outs

    def _ha_commanded_outputs(self) -> dict[tuple[str, str], str]:
        """(evok-2 dev, circuit) -> HA entity name, read from the JSON inside HA's command payloads/templates.
        A switch whose on- and off-payload drive DIFFERENT circuits (the roof window) names them "<name> - on/off action"."""
        pat = re.compile(r'"circuit":\s*"([^"]+)"\s*,\s*"dev":\s*"([^"]+)"|"dev":\s*"([^"]+)"\s*,\s*"circuit":\s*"([^"]+)"')

        def refs(text: str):
            return [((m.group(2) or m.group(3)), (m.group(1) or m.group(4))) for m in pat.finditer(text)]
        found: dict[tuple[str, str], str] = {}
        for plat in ("light", "switch", "fan"):
            for e in self.ha.get(plat, []):
                if not str(e.get("command_topic", "")).startswith(("unipi1/", "unipi/")) or str(e["command_topic"]).startswith("unipi/Neuron_"):
                    continue
                name = e["name"].strip()
                if name.lower().startswith("test"):
                    continue                                          # leftover test entity, not a real function
                on, off = refs(str(e.get("payload_on", ""))), refs(str(e.get("payload_off", "")))
                if plat == "switch" and on and off and on[0] != off[0]:
                    found.setdefault(on[0], f"{name} openen ({self._duration(e.get('payload_on'))})")
                    found.setdefault(off[0], f"{name} sluiten ({self._duration(e.get('payload_off'))})")
                    for (dev, c), payload in ((on[0], e.get("payload_on")), (off[0], e.get("payload_off"))):
                        m = re.search(r'"duration":\s*(\d+)', str(payload))
                        if m:
                            self._timed[f"{DEV_MAP[dev]}/{c}"] = int(m.group(1))
                    continue
                for dev, c in refs(" ".join(str(e.get(k, "")) for k in ("payload_on", "payload_off", "command_on_template", "command_off_template"))):
                    if dev in DEV_MAP:
                        found.setdefault((dev, c), name)
        return found

    @staticmethod
    def _duration(payload) -> str:
        m = re.search(r'"duration":\s*(\d+)', str(payload))
        return f"{m.group(1)} s" if m else "pulse"

    # ---- local rules -----------------------------------------------------------------------------------------------------
    def rules(self) -> list[dict]:
        rules = []
        for e in self.legacy:
            hl = e.get("handle_local")
            if not hl:
                continue
            base = {"name": e["description"], "trigger_dev": "di", "trigger_circuit": self.circuit(e["circuit"]),
                    "trigger_operator": "eq", "trigger_value": "1", "conditions": [], "when": "always"}
            if hl["type"] == "bel":
                rules.append({**base, "group": "Bel", "action_type": "pulse", "action_dev": "ro", "action_circuit": hl["output_circuit"],
                              "action_pulse": {"count": int(hl["rings"]), "on_ms": BELL_ON_MS, "off_ms": BELL_OFF_MS}})
            elif hl["type"] == "dimmer":
                parts = e.get("state_topic", "").split("/")
                room = parts[2].capitalize() if len(parts) > 3 else "Licht"
                rules.append({**base, "group": room, "action_type": "dimmer", "action_dev": "ao", "action_circuit": hl["output_circuit"],
                              "action_value": hl["level"], "dimmer_hold": False, "dimmer_fade_on_ms": 0, "dimmer_fade_off_ms": 0})
            else:
                raise ValueError(f"unsupported handle_local type {hl['type']!r} on {e['circuit']}")
        return rules

    # ---- verification ----------------------------------------------------------------------------------------------------
    def check_against_evok2(self) -> list[str]:
        """Every legacy circuit must exist in the evok-2 snapshot (catches typos in the legacy file)."""
        have = {(x["dev"], x["circuit"]) for x in self.evok2}
        have |= {("output", c) for d, c in have if d == "relay"} | {("analogoutput", c) for d, c in have if d == "ao"}   # legacy REST aliases
        problems = []
        for e in self.legacy:
            dev = {"humidity": "1wdevice"}.get(e["dev"], e["dev"])
            if dev == "temp" and e["circuit"] in self._onewire:
                dev = "1wdevice"
            if self.evok2 and (dev, e["circuit"]) not in have:
                problems.append(f"legacy {e['dev']}/{e['circuit']} ({e['description']}) is not in the evok-2 snapshot")
            hl = e.get("handle_local")
            if hl and self.evok2 and (hl["output_dev"], hl["output_circuit"]) not in have:
                problems.append(f"handle_local target {hl['output_dev']}/{hl['output_circuit']} of {e['description']} is not in the evok-2 snapshot")
        return problems

    def check_against_evok3(self, rest: list[dict]) -> list[str]:
        have = {(x["dev"], x["circuit"]) for x in rest if "circuit" in x}
        problems = []
        for k in self.circuits():
            dev, c = k.split("/")[0], k.split("/")[1]
            if (dev, c) not in have:
                problems.append(f"circuit {k} is not in the evok-3 listing")
        for r in self.rules():
            for dev, c in ((r["trigger_dev"], r["trigger_circuit"]), (r["action_dev"], r["action_circuit"])):
                if (dev, c) not in have:
                    problems.append(f"rule '{r['name']}': {dev}/{c} is not in the evok-3 listing")
        return problems

    # ---- the map for Home Assistant ----------------------------------------------------------------------------------------
    def topics_new(self, key: str) -> tuple[str, str]:
        dev, c, *rest = key.split("/")
        if dev in ("temp",):
            return f"unipi/{self.device}/1-wire/{c}/temp/state", ""
        if dev == "1wdevice":
            return f"unipi/{self.device}/1-wire/{c}/{rest[0]}", ""
        base = f"unipi/{self.device}/{dev}/{c}"
        return base + "/state", (base + "/set") if dev in ("ro", "do", "ao", "led") else ""

    def render_map(self) -> str:
        circuits = self.circuits()
        L = []
        w = L.append
        w("---\ntype: Concept\ntitle: \"L513 migration map: old names -> new names, and what moves to Home Assistant\"\n"
          "description: \"Generated by tools/legacy_to_site.py from the legacy config and the HA export. Old topic/entity -> new circuit/topic; plus the functions that were done by the old script and are now Home Assistant's job.\"\n"
          "tags: [l513, migration, home-assistant]\nstatus: draft\ngenerated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-25T21:00:00Z }\n---\n")
        w(f"Device name used in the new topics: `{self.device}` (check after the first start: it is `<family>_<model>_<serial>`). "
          f"The xS30 extension is assumed to be named `{self.prefix}` in `/etc/evok/config.yaml` (**assumption, T31 verifies it**).\n")
        warn = self.check_against_evok2()
        if warn:
            w("# Warnings (the legacy file refers to something the L513 does not have)\n")
            for p in warn:
                w(f"- {p}")
            w("")
        w("# How to switch Home Assistant over without losing entity ids\n"
          "Where the old entity was a plain mirror of the signal the new entity has the SAME name. HA hands out an entity_id from the name, "
          "so: (1) remove the old YAML entity, (2) delete the orphaned entry in Settings -> Devices & services -> Entities, (3) the discovered entity "
          "takes the same `entity_id` and every automation, dashboard and history card keeps working. Entities that need post-processing "
          "(PIR with hold, lux, water in litres) arrive as *raw* entities with a different name; create the template entity with the OLD name on top of them.\n")
        w("# Old -> new, per input / sensor\n")
        w("| Old HA entity | Old topic | New circuit | New state topic | New HA name (set via `circuits`) | Notes |\n|---|---|---|---|---|---|")
        for e in self.legacy:
            plat, ha = self.ha_for(e.get("state_topic", ""))
            k = self.key(e["dev"], e["circuit"])
            new_topic, _ = self.topics_new(k)
            old_name = (ha or {}).get("name", "(not in HA export)")
            note = []
            if circuits[k].get("inverted"):
                note.append("inverted (NC contact)")
            if e.get("device_delay") and e["dev"] == "input" and e.get("device_type") != "counter":
                note.append(f"PIR hold {e['device_delay']} s -> HA")
            if e["dev"] == "ai":
                note.append("volts; lux = V x 200 -> HA")
            if e.get("device_type") == "counter":
                note.append("ticks; 1 tick = 0.01 L -> HA")
            if "handle_local" in e:
                note.append("local rule" + (f"; the old topic was also the state of light '{old_name}'" if plat == "light" else ""))
            w(f"| {old_name} | `{e.get('state_topic','')}` | `{k}` | `{new_topic}` | {circuits[k].get('name','')} | {'; '.join(note)} |")
        w("\n# Old -> new, per output (lights, relays, window, ventilation)\n")
        w("| Old HA entity | Old command topic | New circuit | New command topic | Notes |\n|---|---|---|---|---|")
        for plat in ("light", "switch", "fan"):
            for e in self.ha.get(plat, []):
                if not str(e.get("command_topic", "")).startswith("unipi1/"):
                    continue
                texts = " ".join(str(e.get(k, "")) for k in ("payload_on", "payload_off", "command_on_template"))
                circs = sorted(set(re.findall(r'"circuit":\s*"([^"]+)"', texts)))
                devs = sorted(set(re.findall(r'"dev":\s*"([^"]+)"', texts)))
                news = [f"{DEV_MAP.get(d, d)}/{c}" for d in devs for c in circs]
                tops = [self.topics_new(n)[1] for n in news]
                w(f"| {e['name'].strip()} | `{e['command_topic']}` | {', '.join('`'+n+'`' for n in news)} | {', '.join('`'+t+'`' for t in tops if t)} | {plat} |")
        w("\n# What the old script did that Home Assistant must do now\n")
        pirs = [e for e in self.legacy if e["dev"] == "input" and e.get("device_delay") and e.get("device_type") != "counter"]
        w("## 1. PIR hold (off-delay)\nThe new bridge publishes the raw input. Make a template `binary_sensor` (or an automation) with `delay_off` per PIR; "
          "the bridge refuses an `off_delay_s` option on purpose (ADR-005).\n")
        w("| PIR | New input | `delay_off` |\n|---|---|---|")
        for e in pirs:
            w(f"| {e['description']} | `{self.key('input', e['circuit'])}` | {int(e['device_delay'])} s |")
        w("\n## 2. Lux (analog inputs)\nOld: the script averaged `interval` samples of the voltage and published `lux = round(mean(V) x 200)` as `{\"lux\": n}`. "
          "New: the bridge publishes the voltage of `ai/<circuit>`. In HA: a `sensor` with template `value x 200`, smoothed with the `statistics` or `filter` integration "
          "(about one reading per minute).\n")
        w("| Sensor | New input | old averaging |\n|---|---|---|")
        for e in self.legacy:
            if e["dev"] == "ai":
                w(f"| {e['description']} | `{self.key('ai', e['circuit'])}` | {e.get('interval','')} samples |")
        wm = [e for e in self.legacy if e.get("device_type") == "counter"]
        w("\n## 3. Water meter\n" + "".join(
            f"Old: `{e['state_topic']}` published `{{\"counter_delta\", \"counter\"}}` every {e.get('device_delay')} s; HA computed litres = delta x 0.01 and m3 = counter x 0.00001. "
            f"New: the bridge publishes the pulse counter of `{self.key('input', e['circuit'])}` as a `total_increasing` sensor in ticks. In HA: m3 = ticks x 0.00001 (template), consumption per period via `utility_meter` or a derivative sensor.\n" for e in wm))
        w("\n## 4. Sensor payloads\nOld JSON objects (`{\"temperature\": n}`, `{\"humidity\": n}`) become plain discovered sensors; the old `value_template`s are no longer needed. Availability is now ONE topic per box (`unipi/<device>/status`) instead of one per sensor.\n")
        w("## 5. Lights\nThe lights become discovered `light` entities (brightness scale and transition are handled by the discovery). The old defaults that HA filled in (on-brightness 199/200 of 255, transition 2-4 s) "
          "now have to be set in the HA light service calls/automations or scenes.\n")
        w("## 6. Doorbell from HA\nOld: switch \"Bel\" sent `{\"state\": \"pulse\", \"repeat\": \"2\"}` and the script timed it. New: publish `{\"pulse\": {\"count\": 2}}` (timing 100/300 ms comes from the circuit defaults) or `{\"preset\": \"ring_2x\"}` to the relay's `/set` topic; a physical button press is handled locally by the rules and needs no HA at all.\n")
        w("## 7. Roof window\nOld switch sent two different timed commands. New: two actions in a HA script/template switch: open = `{\"state\": \"ON\", \"duration_s\": 25}` to the `ro/1_02` `/set` topic, close = `{\"state\": \"ON\", \"duration_s\": 35}` to `ro/1_01`. The state still comes from the window contact input.\n")
        w("## 8. Ventilation\nThe old fan used a percentage scale 1-255 on an analog output (circuits 3_02/3_03). New: the analog output is a discovered light; make a template fan (like the working \"Ventilatie Woonkamer\" on the S103) or use T19.\n")
        w("## 9. Everything else 1:1\nContacts, locks, leak sensors, wall switches without a local rule (e.g. the rear outside light switch): same signal, new topic - point the HA automation at the new entity.\n")
        w("\n# Local rules that stay in the Unipi (work without Home Assistant or the network)\n")
        w("| Rule | Trigger | Action |\n|---|---|---|")
        for r in self.rules():
            act = (f"ring {r['action_pulse']['count']}x ({r['action_pulse']['on_ms']}/{r['action_pulse']['off_ms']} ms)" if r["action_type"] == "pulse"
                   else f"toggle `ao/{r['action_circuit']}` between 0 V and {r['action_value']} V")
            w(f"| {r['name']} | `di/{r['trigger_circuit']}` pressed | {act} |")
        return "\n".join(L) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--legacy", required=True)
    ap.add_argument("--ha-entities")
    ap.add_argument("--evok2-rest")
    ap.add_argument("--evok3-rest", help="evok-3 /rest/all of the real card: verify every generated circuit exists")
    ap.add_argument("--uart-prefix", default="xS30")
    ap.add_argument("--device-name", default="Neuron_L513_10")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--map-doc")
    ap.add_argument("--strict", action="store_true", help="exit 1 when a circuit does not exist in the snapshot(s)")
    a = ap.parse_args(argv)
    conv = Converter(json.load(open(a.legacy)), json.load(open(a.ha_entities)) if a.ha_entities else None,
                     json.load(open(a.evok2_rest)) if a.evok2_rest else None, a.uart_prefix, a.device_name)
    problems = conv.check_against_evok2()
    if a.evok3_rest:
        problems += conv.check_against_evok3(json.load(open(a.evok3_rest)))
    out = Path(a.out_dir); out.mkdir(parents=True, exist_ok=True)
    (out / "circuits.json").write_text(json.dumps(conv.circuits(), indent=2, ensure_ascii=False) + "\n")
    (out / "local_rules.json").write_text(json.dumps(conv.rules(), indent=2, ensure_ascii=False) + "\n")
    if a.map_doc:
        Path(a.map_doc).write_text(conv.render_map(), encoding="utf-8")
    print(f"{len(conv.circuits())} circuits, {len(conv.rules())} local rules written to {out}")
    for p in problems:
        print("WARNING:", p, file=sys.stderr)
    return 1 if (problems and a.strict) else 0


if __name__ == "__main__":
    sys.exit(main())
