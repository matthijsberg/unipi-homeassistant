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
