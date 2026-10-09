"""Static checks of web/index.html (no browser available): the script parses, blocks are wired up, and the
save code carries unknown rule fields through (it used to rebuild rules from scratch and erase them)."""
import re
from pathlib import Path

import pytest

HTML = (Path(__file__).resolve().parent.parent / "web" / "index.html").read_text(encoding="utf-8")
SCRIPT = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", HTML, re.S)[0]


def test_inline_script_parses():
    esprima = pytest.importorskip("esprima")
    esprima.parseScript(SCRIPT, tolerant=False)


def test_every_block_is_defined_in_toolbox_and_loadable():
    defined = set(re.findall(r"Blockly\.Blocks\['(unipi_\w+)'\]", HTML))
    toolbox = set(re.findall(r'"type": "(unipi_\w+)"', HTML))
    created = set(re.findall(r"newBlock\('(unipi_\w+)'\)", HTML))
    assert defined == toolbox and created <= defined
    assert {"unipi_action_pulse", "unipi_action_toggle"} <= defined


def test_save_keeps_unknown_fields_and_the_rule_id():
    assert "ruleBlock.data = JSON.stringify(rule)" in SCRIPT            # load remembers the full rule
    assert "JSON.parse(block.data)" in SCRIPT and "Object.assign({}, base" in SCRIPT   # save starts from it


@pytest.mark.parametrize("field", ["WHEN", "LEVEL", "HOLD", "PRESET", "COUNT", "ON_MS", "OFF_MS"])
def test_new_fields_are_used_for_both_saving_and_loading(field):
    assert len(re.findall(rf"['\"]{field}['\"]", SCRIPT)) >= 3          # defined once, read on save, set on load


@pytest.mark.parametrize("key", ["action_pulse", "action_preset", "dimmer_hold", "when"])
def test_saved_rule_contains_the_new_keys(key):
    assert key in SCRIPT


# ---- rule names + activity panel (T17b) ------------------------------------------------------------------------------
def test_dropdowns_offer_evok3_device_names_only():
    legacy = re.findall(r'\["[^"]+", "(?:input|relay|output|analogoutput)"\]', HTML)
    assert legacy == []
    for pair in ('["Digital Input", "di"]', '["Relay", "ro"]', '["Digital Output", "do"]', '["Analog Output", "ao"]'):
        assert pair in HTML


def test_saved_legacy_names_are_mapped_when_loading():
    assert "const LEGACY_DEV" in SCRIPT and "function canonDev" in SCRIPT
    for field in ("rule.trigger_dev", "cond.dev", "rule.action_dev"):
        assert f"canonDev({field})" in SCRIPT, field
    assert "setFieldValue(rule.trigger_dev" not in SCRIPT and "setFieldValue(rule.action_dev" not in SCRIPT


def test_activity_panel_is_wired_to_the_trace_api():
    assert 'id="ruleActivity"' in HTML and 'id="clearActivityBtn"' in HTML
    assert "/api/rule_trace?since=" in SCRIPT and "setInterval(pollRuleTrace" in SCRIPT
    for cls in ("unipi-flash-ok", "unipi-flash-hit", "unipi-flash-stop"):
        assert f".{cls}" in HTML and f"'{cls}'" in SCRIPT


def test_activity_rows_never_inject_server_text_as_html():
    body = re.search(r"function addActivityRow\(e\) \{(.*?)\n            \}\n", SCRIPT, re.S).group(1)
    assert "innerHTML" not in body and "textContent" in body            # rule names / details come from the bridge


def test_saving_reloads_rules_and_shows_the_servers_refusal_reason():
    save = re.search(r"async function saveRules\(\) \{(.*?)\n            \}\n", SCRIPT, re.S).group(1)
    assert "await loadRules()" in save and "Rules NOT saved" in save and "(await res.json()).error" in save


def test_disabled_rules_show_their_reason_on_the_block_and_it_is_never_saved_back():
    assert "setWarningText" in SCRIPT and "rule.disabled_reason" in SCRIPT
    assert "delete base.disabled_reason" in SCRIPT
