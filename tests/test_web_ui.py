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


@pytest.mark.parametrize("field", ["LEVEL", "HOLD", "HOLD_MS", "SPEED", "MINV", "FADE_ON", "FADE_OFF", "PRESET", "COUNT", "ON_MS", "OFF_MS"])
def test_new_fields_are_used_for_both_saving_and_loading(field):
    assert len(re.findall(rf"['\"]{field}['\"]", SCRIPT)) >= 3          # defined once, read on save, set on load


@pytest.mark.parametrize("key", ["action_pulse", "action_preset", "dimmer_hold", "dimmer_hold_ms", "dimmer_speed", "dimmer_min", "dimmer_fade_on_ms", "dimmer_fade_off_ms"])
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


def test_the_ha_fallback_selector_is_gone_but_existing_rules_keep_their_setting():
    assert '"WHEN"' not in SCRIPT and "'WHEN'" not in SCRIPT                  # no dropdown (and no code touching a missing field)
    assert "ha_offline" not in SCRIPT                                        # nothing in the editor mentions it any more
    assert "Object.assign({}, base" in SCRIPT                                # `when` of an existing rule travels in `base`


def test_push_to_dim_block_is_discoverable_and_tells_the_truth_about_the_trigger():
    assert "Push-to-dim light" in HTML and "follows press and release" in HTML
    assert "(actionType === 'dimmer' && dimmerHold) ? 'any' : triggerOp" in SCRIPT


# ---- zoom, group tabs, pinned local Blockly (T17f) --------------------------------------------------------------------------
import hashlib  # noqa: E402

STATIC = Path(__file__).resolve().parent.parent / "web" / "static"
BLOCKLY = STATIC / "blockly-13.3.0.min.js"


def test_blockly_is_pinned_and_served_from_this_box():
    assert '<script src="/static/blockly-13.3.0.min.js"></script>' in HTML
    assert "unpkg.com/blockly/blockly" not in HTML and "https://unpkg.com" not in SCRIPT      # nothing is fetched from the internet
    data = BLOCKLY.read_bytes()
    assert len(data) > 500_000
    notice = (STATIC / "BLOCKLY-NOTICE.txt").read_text()
    assert hashlib.sha256(data).hexdigest() in notice and "13.3.0" in notice and "Apache" in notice


def test_every_blockly_function_the_editor_relies_on_exists_in_the_pinned_build():
    js = BLOCKLY.read_text(encoding="utf-8", errors="ignore")
    for name in ("zoomToFit", "setCollapsed", "getHeightWidth", "getRelativeToSurfaceXY", "getSvgRoot", "moveBy", "getTopBlocks",
                 "getBlockById", "setWarningText", "BLOCK_CHANGE", "BLOCK_CREATE", "scaleSpeed", "startScale", "maxScale", "minScale", "pinch"):
        assert name in js, name


def test_the_editor_never_calls_the_block_setvisible_that_blockly_13_does_not_have():
    assert not re.search(r"(block|b|root)\.setVisible\(", SCRIPT)          # hiding is done on the SVG element instead
    assert "root.style.display = show ? '' : 'none'" in SCRIPT


def test_zoom_is_enabled_with_controls_and_ctrl_wheel():
    assert re.search(r"zoom:\s*\{[^}]*controls:\s*true[^}]*wheel:\s*true[^}]*pinch:\s*true", SCRIPT)
    assert "move: { scrollbars: true, drag: true, wheel: true }" in SCRIPT


def test_view_bar_has_tabs_search_and_the_four_helpers():
    for el in ('id="viewBar"', 'id="groupTabs"', 'id="ruleSearch"', "arrangeRules()", "zoomFit()", "collapseRules(true)", "collapseRules(false)"):
        assert el in HTML, el
    for fn in ("function rebuildTabs", "function applyView", "function arrangeRules", "function zoomFit", "function collapseRules"):
        assert fn in SCRIPT, fn


def test_tabs_only_hide_rules_so_saving_still_saves_everything():
    save = re.search(r"async function saveRules\(\) \{(.*?)\n            \}\n", SCRIPT, re.S).group(1)
    assert "getTopBlocks(false)" in save and "display" not in save and "activeGroup" not in save      # save ignores what is shown
    assert "group: group" in save and "GROUP" in save


def test_group_names_are_never_injected_as_html():
    body = re.search(r"function rebuildTabs\(\) \{(.*?)\n            \}\n", SCRIPT, re.S).group(1)
    assert "innerHTML" not in body and body.count("textContent") >= 3


def test_load_keeps_the_saved_order_and_restores_groups():
    assert "ruleBlock.unipiOrder = nextOrder++" in SCRIPT and "nextOrder = 0;" in SCRIPT
    assert "ruleBlock.setFieldValue(rule.group || '', 'GROUP')" in SCRIPT


def test_new_rule_joins_the_open_tab():
    assert "Blockly.Events.BLOCK_CREATE" in SCRIPT and "b.setFieldValue(activeGroup, 'GROUP')" in SCRIPT
