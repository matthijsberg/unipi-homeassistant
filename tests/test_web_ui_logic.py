"""Executes the REAL inline script of web/index.html (QuickJS) against a fake browser and a fake Blockly
(tests/js_harness.js) and checks what it does: load -> show -> save round trips, tabs, search, activity panel.
These are the tests that guard the editor against losing rules."""
import json
import re
from pathlib import Path

import pytest

quickjs = pytest.importorskip("quickjs")

ROOT = Path(__file__).resolve().parent.parent
HTML = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
SCRIPT = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", HTML, re.S)[0]
HARNESS = (Path(__file__).resolve().parent / "js_harness.js").read_text()


def base(**kw):
    r = dict(name="r", trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value="1", conditions=[],
             action_type="set", action_dev="ao", action_circuit="1_01", action_value="5", action_transition=0.0,
             action_delay=0.0, action_pulse=None, action_preset=None, when="always", dimmer_hold=True, group="")
    r.update(kw)
    return r


class Editor:
    def __init__(self, rules, trace=None):
        self.ctx = quickjs.Context()
        self.ctx.eval(HARNESS)
        self.ctx.eval("__state.rules = %s; __state.trace = %s;" % (json.dumps(rules), json.dumps(trace or [])))
        self.ctx.eval(SCRIPT)
        self.settle()

    def settle(self):
        for _ in range(2000):
            if not self.ctx.execute_pending_job():
                break

    def js(self, code):
        out = self.ctx.eval("JSON.stringify((function(){ return %s })())" % code)
        return None if out is None else json.loads(out)

    def run(self, code):
        self.ctx.eval(code)
        self.settle()

    def save(self):
        self.run("__state.putBody = null; saveRules()")
        return self.js("__state.putBody")

    def rules_blocks(self):
        return self.js("workspace.getTopBlocks(true).filter(b=>b.type==='unipi_rule').map(b=>({id:b.id,name:b.getFieldValue('NAME'),group:b.getFieldValue('GROUP'),"
                       "y:b.xy.y,h:b.getHeightWidth().height,shown:b.root.style.display!=='none',warning:b.warning}))")

    def tabs(self):
        return self.js("document.getElementById('groupTabs').children.map(c=>({label:c.textContent,active:c.className==='active'}))")

    def click_tab(self, label):
        self.run("document.getElementById('groupTabs').children.find(c=>c.textContent.indexOf(%s)===0).onclick()" % json.dumps(label))

    def errors(self):
        return self.js("__errors")


RULES = [
    base(id="a", name="Serre Light", group="Serre", trigger_dev="input", trigger_circuit="xS51_03", action_dev="analogoutput",
         action_value="0.5", action_transition=3000.0),
    base(id="b", name="Serre Dim", group="Serre", action_type="dimmer", action_value=8, dimmer_hold=True, dimmer_hold_ms=900,
         dimmer_speed=3.0, dimmer_min=1.5, dimmer_fade_on_ms=1200, dimmer_fade_off_ms=800, trigger_operator="any"),
    base(id="c", name="Doorbell", group="Voordeur", trigger_circuit="2_05", action_type="pulse", action_dev="ro", action_circuit="2_02",
         action_value=None, action_pulse={"count": 3, "on_ms": 100, "off_ms": 250}),
    base(id="d", name="Odd one", group="", action_type="toggle", action_dev="relay", action_circuit="3_01", action_value=None,
         when="ha_offline", x_future={"keep": [1, 2]}, disabled_reason="because"),
]


# ---- booting and loading ----------------------------------------------------------------------------------------------------
def test_the_page_script_boots_and_loads_every_rule_without_errors():
    ed = Editor(RULES)
    assert ed.errors() == []
    rb = ed.rules_blocks()
    assert [r["name"] for r in rb] == ["Serre Light", "Serre Dim", "Doorbell", "Odd one"]
    opts = ed.js("Blockly.lastOptions")
    assert opts["zoom"]["controls"] is True and opts["zoom"]["wheel"] is True and opts["zoom"]["pinch"] is True
    assert opts["move"] == {"scrollbars": True, "drag": True, "wheel": True}


def test_old_evok2_device_names_are_shown_with_the_evok3_names():
    ed = Editor(RULES)
    fields = ed.js("(function(){const r=workspace.getTopBlocks(true)[0]; const t=r.getInputTargetBlock('TRIGGER'), a=r.getInputTargetBlock('ACTION');"
                   " return {t:t.getFieldValue('DEV'), a:a.getFieldValue('DEV'), val:a.getFieldValue('VALUE'), tr:a.getFieldValue('TRANSITION')}})()")
    assert fields == {"t": "di", "a": "ao", "val": "0.5", "tr": 3000}


def test_a_disabled_rule_shows_its_reason_on_the_block():
    ed = Editor(RULES)
    assert [r["warning"] for r in ed.rules_blocks()][3] == "DISABLED - this rule does nothing until fixed: because"


def test_rules_are_stacked_without_overlap_after_loading():
    rb = Editor(RULES).rules_blocks()
    for a, b in zip(rb, rb[1:]):
        assert b["y"] >= a["y"] + a["h"], (a, b)


# ---- the data-loss guard: load -> save must hand everything back --------------------------------------------------------------
def test_saving_returns_every_rule_with_ids_unknown_fields_and_new_settings():
    ed = Editor(RULES)
    put = ed.save()
    assert [r["id"] for r in put] == ["a", "b", "c", "d"]
    a, b, c, d = put
    assert (a["trigger_dev"], a["action_dev"], a["group"]) == ("di", "ao", "Serre")                  # canonical names written back
    assert (a["action_value"], a["action_transition"]) == ("0.5", 3000)
    assert b["action_type"] == "dimmer" and b["trigger_operator"] == "any"                            # push-to-dim follows both edges
    assert (b["dimmer_hold_ms"], b["dimmer_speed"], b["dimmer_min"], b["dimmer_fade_on_ms"], b["dimmer_fade_off_ms"]) == (900, 3, 1.5, 1200, 800)
    assert b["action_value"] == 8 and b["dimmer_hold"] is True
    assert c["action_type"] == "pulse" and c["action_pulse"] == {"count": 3, "on_ms": 100, "off_ms": 250} and c["action_preset"] is None
    assert d["when"] == "ha_offline" and d["x_future"] == {"keep": [1, 2]}                            # fields the editor has no block for survive
    assert "disabled_reason" not in d                                                                 # computed by the bridge, never saved
    assert ed.errors() == []


def test_save_then_reload_shows_the_same_rules_again():
    ed = Editor(RULES)
    ed.save()
    names = [r["name"] for r in ed.rules_blocks()]
    assert names == ["Serre Light", "Serre Dim", "Doorbell", "Odd one"] and ed.errors() == []
    assert [r["id"] for r in ed.save()] == ["a", "b", "c", "d"]


def test_a_refused_save_shows_the_reason_and_keeps_the_screen():
    ed = Editor(RULES)
    ed.run("__state.putStatus = 400; __state.putError = 'count 50 outside 1..6'")
    ed.save()
    assert "count 50 outside 1..6" in ed.js("document.getElementById('statusMsg').textContent")
    assert len(ed.rules_blocks()) == 4


# ---- tabs, search, arrange ------------------------------------------------------------------------------------------------------
def test_tabs_are_built_from_the_groups_with_counts():
    assert [t["label"] for t in Editor(RULES).tabs()] == ["All4", "Serre2", "Voordeur1", "No group1"]


def test_a_tab_hides_other_rules_stacks_the_rest_and_saving_still_saves_everything():
    ed = Editor(RULES)
    ed.click_tab("Serre")
    rb = ed.rules_blocks()
    assert [(r["name"], r["shown"]) for r in rb] == [("Serre Light", True), ("Serre Dim", True), ("Doorbell", False), ("Odd one", False)]
    shown = [r for r in rb if r["shown"]]
    assert shown[0]["y"] == 20 and shown[1]["y"] >= shown[0]["y"] + shown[0]["h"]                   # compact, no gap, no overlap
    assert [r["id"] for r in ed.save()] == ["a", "b", "c", "d"]                                       # hidden rules are saved too
    ed.click_tab("All")
    assert all(r["shown"] for r in ed.rules_blocks())


def test_no_group_tab_and_unknown_group_fallback():
    ed = Editor(RULES)
    ed.click_tab("No group")
    assert [r["name"] for r in ed.rules_blocks() if r["shown"]] == ["Odd one"]


def test_search_matches_names_circuits_and_groups():
    ed = Editor(RULES)
    for needle, expected in (("xs51_03", ["Serre Light"]), ("doorbell", ["Doorbell"]), ("voordeur", ["Doorbell"]), ("serre", ["Serre Light", "Serre Dim"]), ("nothing", [])):
        ed.run("document.getElementById('ruleSearch').value = %s; applyView()" % json.dumps(needle))
        assert [r["name"] for r in ed.rules_blocks() if r["shown"]] == expected, needle
    ed.run("document.getElementById('ruleSearch').value = ''; applyView()")
    assert len([r for r in ed.rules_blocks() if r["shown"]]) == 4


def test_a_new_rule_joins_the_open_tab_and_renaming_a_group_updates_the_tabs():
    ed = Editor(RULES)
    ed.click_tab("Voordeur")
    ed.run("workspace.newBlock('unipi_rule')")
    assert ed.js("workspace.getTopBlocks(false).filter(b=>b.type==='unipi_rule').pop().getFieldValue('GROUP')") == "Voordeur"
    ed.run("workspace.getTopBlocks(false).filter(b=>b.type==='unipi_rule')[2].setFieldValue('Bel','GROUP')")
    labels = [t["label"] for t in ed.tabs()]
    assert "Bel1" in labels and ed.errors() == []


def test_collapse_all_and_arrange_keep_everything_visible_and_tidy():
    ed = Editor(RULES)
    before = sum(r["h"] for r in ed.rules_blocks())
    ed.run("collapseRules(true); __runTimeouts()")
    rb = ed.rules_blocks()
    assert sum(r["h"] for r in rb) < before and all(a["y"] + a["h"] <= b["y"] for a, b in zip(rb, rb[1:]))
    ed.run("collapseRules(false); __runTimeouts()")
    assert sum(r["h"] for r in ed.rules_blocks()) == before


def test_fit_button_zooms_to_fit():
    ed = Editor(RULES)
    ed.run("zoomFit()")
    assert ed.js("Blockly.ws.zoomedToFit") == 1


# ---- the activity panel ---------------------------------------------------------------------------------------------------------
TRACE = [
    {"seq": 1, "ts": 1.0, "rule_id": "a", "rule": "Serre Light", "step": "trigger", "ok": True, "detail": "di/xS51_03 = 1 - trigger matched"},
    {"seq": 2, "ts": 1.1, "rule_id": "a", "rule": "Serre Light", "step": "executed", "ok": True, "detail": "set ao/1_01 = 0.5 sent <img src=x onerror=alert(1)>"},
    {"seq": 3, "ts": 2.0, "rule_id": "c", "rule": "Doorbell", "step": "conditions", "ok": False, "detail": "condition stopped it"},
]


def test_activity_rows_flash_the_rule_block_and_never_inject_html():
    ed = Editor(RULES, TRACE)
    ed.run("pollRuleTrace()")
    rows = ed.js("document.getElementById('ruleActivity').children.map(r=>r.textContent)")
    assert len(rows) == 3 and "Doorbell" in rows[0] and "<img src=x onerror=alert(1)>" in rows[1]    # newest first, shown as plain text
    assert ed.js("document.getElementById('ruleActivity').innerHTMLSet.some(x => x.indexOf('<img') >= 0)") is False
    roots = ed.js("workspace.getTopBlocks(false).filter(b=>b.type==='unipi_rule').map(b=>({n:b.getFieldValue('NAME'),ok:b.root.classList.contains('unipi-flash-ok'),"
                  "hit:b.root.classList.contains('unipi-flash-hit'),stop:b.root.classList.contains('unipi-flash-stop')}))")
    by = {r["n"]: r for r in roots}
    assert by["Serre Light"]["ok"] and by["Doorbell"]["stop"] and not by["Serre Dim"]["ok"]


def test_the_poller_only_asks_for_new_events():
    ed = Editor(RULES, TRACE)
    ed.run("pollRuleTrace()")
    ed.run("pollRuleTrace()")
    urls = [c["url"] for c in ed.js("__calls") if "rule_trace" in c["url"]]
    assert urls == ["/api/rule_trace?since=0", "/api/rule_trace?since=3"]
    assert ed.js("document.getElementById('ruleActivity').children.length") == 3                     # no duplicates


def test_the_live_state_poller_runs_without_errors():
    ed = Editor(RULES)
    ed.run("pollLiveStates()")
    assert ed.errors() == []


def test_unknown_values_in_a_saved_rule_do_not_break_loading():
    ed = Editor([base(id="z", name="weird", trigger_dev="foo", trigger_operator="zzz", action_dev="bar", action_value=None)])
    assert ed.errors() == [] and len(ed.rules_blocks()) == 1


# ---- what the user EDITS in the editor is what gets saved (not the copy of the old rule) -------------------------------------------
def action_block(ed, rule_index):
    return "workspace.getTopBlocks(true).filter(b=>b.type==='unipi_rule')[%d].getInputTargetBlock('ACTION')" % rule_index


def test_edits_in_the_editor_win_over_the_loaded_rule():
    ed = Editor(RULES)
    rule = "workspace.getTopBlocks(true).filter(b=>b.type==='unipi_rule')"
    ed.run(f"{rule}[1].setFieldValue('Serre Dim v2','NAME'); {rule}[1].setFieldValue('Bijkeuken','GROUP')")
    ed.run(f"{rule}[1].getInputTargetBlock('TRIGGER').setFieldValue('xS51_09','CIRCUIT')")
    for field, value in (("LEVEL", 4.5), ("HOLD_MS", 1500), ("SPEED", 4), ("MINV", 2), ("FADE_ON", 2500), ("FADE_OFF", 3500), ("HOLD", "FALSE")):
        ed.run(f"{action_block(ed, 1)}.setFieldValue({json.dumps(value)}, '{field}')")
    ed.run(f"{action_block(ed, 2)}.setFieldValue(5, 'COUNT'); {action_block(ed, 2)}.setFieldValue(150, 'ON_MS'); {action_block(ed, 2)}.setFieldValue(400, 'OFF_MS')")
    ed.run(f"{action_block(ed, 0)}.setFieldValue('2', 'VALUE'); {action_block(ed, 0)}.setFieldValue(1500, 'TRANSITION')")
    ed.run(f"{action_block(ed, 3)}.setFieldValue('ro', 'DEV'); {action_block(ed, 3)}.setFieldValue('9_09', 'CIRCUIT')")
    a, b, c, d = ed.save()
    assert (b["name"], b["group"], b["trigger_circuit"]) == ("Serre Dim v2", "Bijkeuken", "xS51_09")
    assert (b["action_value"], b["dimmer_hold_ms"], b["dimmer_speed"], b["dimmer_min"]) == (4.5, 1500, 4, 2)
    assert (b["dimmer_fade_on_ms"], b["dimmer_fade_off_ms"], b["dimmer_hold"]) == (2500, 3500, False)
    assert b["trigger_operator"] == "any" or b["trigger_operator"] == "eq"                          # hold off: the operator is the user's own
    assert c["action_pulse"] == {"count": 5, "on_ms": 150, "off_ms": 400}
    assert (a["action_value"], a["action_transition"]) == ("2", 1500)
    assert (d["action_dev"], d["action_circuit"]) == ("ro", "9_09") and d["when"] == "ha_offline" and d["x_future"] == {"keep": [1, 2]}


def test_switching_a_pulse_to_a_preset_clears_the_inline_numbers():
    ed = Editor(RULES)
    ed.run(f"{action_block(ed, 2)}.setFieldValue('ring_front', 'PRESET')")
    c = ed.save()[2]
    assert c["action_preset"] == "ring_front" and c["action_pulse"] is None


def test_a_rule_without_trigger_or_action_is_not_saved_half_finished():
    ed = Editor(RULES)
    ed.run("workspace.newBlock('unipi_rule')")                                                    # dragged out, nothing connected
    assert [r["id"] for r in ed.save()] == ["a", "b", "c", "d"]
