"""T05: --check-config and the state-dir override (UNIPI_HA_STATE_DIR) - defaults stay exactly as on the S103."""
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GOOD = {"mqtt": {"broker": "mqtt.invalid", "password": "s3cret"}, "websocket": {"url": "ws://127.0.0.1:8080/ws"},  # pii-ok (fake test data)
        "unipi-http": {"url": "http://127.0.0.1:8080/rest/all"}}


def cli(cfg, env=None):
    r = subprocess.run([sys.executable, str(ROOT / "hass-unipi.py"), "--check-config", "--config", str(cfg)],
                       capture_output=True, text=True, env=dict(os.environ, **(env or {})))
    return r.returncode, r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""


def test_valid_config_prints_a_summary_without_secrets(tmp_path):
    (tmp_path / "config.json").write_text(json.dumps(GOOD))
    code, line = cli(tmp_path / "config.json")
    assert code == 0 and line.startswith("OK ") and "mqtt=mqtt.invalid:1883" in line and "0 rule(s)" in line
    assert "s3cret" not in line


def test_invalid_config_exits_2_and_names_the_problem(tmp_path):
    (tmp_path / "config.json").write_text('{"mqtt": {"broker": "x"}}')
    code, line = cli(tmp_path / "config.json")
    assert code == 2 and line.startswith("INVALID config")
    (tmp_path / "config.json").write_text("{not json")
    code, line = cli(tmp_path / "config.json")
    assert code == 2 and "not valid JSON" in line and "\n" not in line                    # one line: callers read the last one


def test_rules_next_to_the_config_are_validated_and_counted(tmp_path):
    (tmp_path / "config.json").write_text(json.dumps(GOOD))
    rule = dict(name="r", trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value=1,
                action_type="set", action_dev="ao", action_circuit="1_01", action_value="5")
    (tmp_path / "local_rules.json").write_text(json.dumps([rule, rule]))
    code, line = cli(tmp_path / "config.json")
    assert code == 0 and "2 rule(s)" in line
    (tmp_path / "local_rules.json").write_text(json.dumps([{"name": "no trigger"}]))
    code, line = cli(tmp_path / "config.json")
    assert code == 2 and line.startswith("INVALID rules")
    (tmp_path / "local_rules.json").write_text('{"not": "a list"}')
    assert cli(tmp_path / "config.json")[0] == 2


def test_a_missing_config_file_is_invalid_not_a_crash(tmp_path):
    code, line = cli(tmp_path / "nope.json")
    assert code == 2 and line.startswith("INVALID config")


def test_check_config_starts_nothing(tmp_path):
    """It must not create logs, caches or state files next to the config."""
    (tmp_path / "config.json").write_text(json.dumps(GOOD))
    before = sorted(os.listdir(tmp_path))
    cli(tmp_path / "config.json")
    assert sorted(os.listdir(tmp_path)) == before


def test_state_dir_env_moves_runtime_state_and_unset_keeps_the_old_places(mod, tmp_path, monkeypatch):
    (tmp_path / "etc").mkdir(); (tmp_path / "var").mkdir()
    cfg = mod.AppConfig(mqtt={"broker": "mqtt.invalid"}, websocket={"url": "ws://127.0.0.1:1/ws"},
                        unipi_http={"url": "http://127.0.0.1:1/rest/all"}, logging={"file_path": str(tmp_path / "b.log")},
                        web_server={"enabled": False})
    monkeypatch.delenv("UNIPI_HA_STATE_DIR", raising=False)
    b = mod.UnipiBridge(cfg, str(tmp_path / "etc" / "config.json"))
    assert b._dimmer_state_file == str(tmp_path / "etc" / "local_rules_state.json")          # unchanged default: next to the rules
    assert b.ext_watchdog.state_file == str(tmp_path / "etc" / "extension_watchdog_state.json")
    monkeypatch.setenv("UNIPI_HA_STATE_DIR", str(tmp_path / "var"))
    b = mod.UnipiBridge(cfg, str(tmp_path / "etc" / "config.json"))
    assert b._dimmer_state_file == str(tmp_path / "var" / "local_rules_state.json")
    assert b.ext_watchdog.state_file == str(tmp_path / "var" / "extension_watchdog_state.json")


def test_device_name_cache_follows_the_env_at_import_time(tmp_path):
    code = ("import importlib.util,sys;s=importlib.util.spec_from_file_location('hu',sys.argv[1]);m=importlib.util.module_from_spec(s);"
            "s.loader.exec_module(m);print(m.DEVICE_NAME_CACHE_FILE)")
    out = lambda env: subprocess.run([sys.executable, "-c", code, str(ROOT / "hass-unipi.py")], capture_output=True, text=True,
                                     env={**{k: v for k, v in os.environ.items() if k != "UNIPI_HA_STATE_DIR"}, **env}).stdout.strip().splitlines()[-1]
    assert out({}) == str(ROOT / ".device_name")
    assert out({"UNIPI_HA_STATE_DIR": str(tmp_path)}) == str(tmp_path / ".device_name")
