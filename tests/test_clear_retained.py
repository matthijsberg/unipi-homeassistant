"""Retained-message cleanup: dry run by default, backup before any change, protected topics stay, restore works."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import clear_retained as cr  # noqa: E402


class FakeBroker:
    """Just enough of a broker: a retained store, subscribe with # patterns, publish with retain (empty payload clears)."""
    def __init__(self, retained, deny_clear=()):
        self.retained, self.deny_clear, self.published = dict(retained), set(deny_clear), []

    def client(self):
        broker = self

        class Msg:
            def __init__(s, t, p, retain=True):
                s.topic, s.payload, s.retain, s.qos = t, p, retain, 1

        class Pub:
            def wait_for_publish(s, timeout=None):
                return True

        class C:
            on_connect = on_message = None
            def connect(s, host, port, ka=60): pass
            def loop_start(s):
                if s.on_connect:
                    s.on_connect(s, None, None, 0)
            def loop_stop(s): pass
            def disconnect(s): pass
            def subscribe(s, pat, qos=0):
                prefix = pat[:-1] if pat.endswith("#") else pat
                for t, p in list(broker.retained.items()):
                    if t.startswith(prefix) and s.on_message:
                        s.on_message(s, None, Msg(t, p))
            def publish(s, topic, payload, qos=0, retain=False):
                broker.published.append((topic, payload, retain))
                if retain and topic not in broker.deny_clear:
                    if payload:
                        broker.retained[topic] = payload
                    else:
                        broker.retained.pop(topic, None)
                return Pub()
        return C()


@pytest.fixture
def setup(tmp_path):
    cfgf = tmp_path / "config.json"; cfgf.write_text(json.dumps({"mqtt": {"broker": "x", "port": 1883, "username": "u", "password": "pw-not-printed"}}))  # pii-ok (fake test data)
    legacy = tmp_path / "legacy.json"; legacy.write_text(json.dumps([{"state_topic": "unipi/bgg/hal/motion"}, {"state_topic": "unipi/bgg/serre/lux"}]))
    retained = {"unipi2/veranda/licht": b'{"state": "off"}', "unipi2/veranda/relay1/set": b'{"state": "on"}', "unipi/bgg/serre/vleugel_huis/contact": b"OFF",
                "unipi/bgg/hal/motion/available": b"online", "unipi/bgg/hal/motion": b"OFF", "unipi1/bgg/hal/bel": b'{"state": "off"}'}
    return tmp_path, cfgf, legacy, FakeBroker(retained)


def run(setup, *args):
    tmp, cfgf, legacy, broker = setup
    return cr.main(["--config", str(cfgf), "--wait", "0", *args], factory=broker.client)


def test_dry_run_changes_nothing_and_says_what_would_go(setup, capsys):
    tmp, cfgf, legacy, broker = setup
    assert run(setup, "--patterns", "unipi2/#", "unipi/#", "--keep-state-topics", str(legacy)) == 0
    out = capsys.readouterr().out
    assert "clear unipi2/veranda/licht" in out and "clear unipi/bgg/serre/vleugel_huis/contact" in out and "dry run" in out
    assert broker.published == [] and len(broker.retained) == 6


def test_protected_state_topics_and_their_available_are_never_cleared(setup):
    tmp, cfgf, legacy, broker = setup
    assert run(setup, "--patterns", "unipi2/#", "unipi/#", "--keep-state-topics", str(legacy), "--backup", str(tmp / "b.json"), "--apply") == 0
    assert "unipi/bgg/hal/motion" in broker.retained and "unipi/bgg/hal/motion/available" in broker.retained
    assert "unipi2/veranda/licht" not in broker.retained and "unipi/bgg/serre/vleugel_huis/contact" not in broker.retained
    assert "unipi1/bgg/hal/bel" in broker.retained                                               # not matched by the patterns at all


def test_the_backup_is_written_before_clearing_and_is_private(setup):
    tmp, cfgf, legacy, broker = setup
    run(setup, "--patterns", "unipi2/#", "--backup", str(tmp / "b.json"), "--apply")
    data = json.loads((tmp / "b.json").read_text())
    assert sorted(m["topic"] for m in data["messages"]) == ["unipi2/veranda/licht", "unipi2/veranda/relay1/set"]
    assert oct((tmp / "b.json").stat().st_mode & 0o777) == "0o600"


def test_apply_without_backup_is_refused(setup):
    with pytest.raises(SystemExit):
        run(setup, "--patterns", "unipi2/#", "--apply")
    assert setup[3].published == []


def test_restore_puts_every_message_back(setup):
    tmp, cfgf, legacy, broker = setup
    before = dict(broker.retained)
    run(setup, "--patterns", "unipi2/#", "unipi/#", "--keep-state-topics", str(legacy), "--backup", str(tmp / "b.json"), "--apply")
    assert broker.retained != before
    assert run(setup, "--restore", str(tmp / "b.json")) == 0
    assert broker.retained == before


def test_a_topic_that_cannot_be_cleared_is_reported_not_claimed_as_done(setup, capsys):
    tmp, cfgf, legacy, broker = setup
    broker.deny_clear = {"unipi2/veranda/licht"}                                                   # e.g. no publish permission
    assert run(setup, "--patterns", "unipi2/#", "--backup", str(tmp / "b.json"), "--apply") == 1
    assert "still retained" in capsys.readouterr().out


def test_the_password_is_never_printed(setup, capsys):
    run(setup, "--patterns", "unipi2/#")
    assert "pw-not-printed" not in capsys.readouterr().out


def test_keep_prefix_protects_whole_trees(setup):
    tmp, cfgf, legacy, broker = setup
    run(setup, "--patterns", "unipi2/#", "--keep-prefix", "unipi2/veranda/relay", "--backup", str(tmp / "b.json"), "--apply")
    assert "unipi2/veranda/relay1/set" in broker.retained and "unipi2/veranda/licht" not in broker.retained
