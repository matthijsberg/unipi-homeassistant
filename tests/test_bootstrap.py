"""T05: bootstrap.sh, site bundle, release-layout deploy/rollback - all run for real in temp trees (no root, no systemd)."""
import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"

STUB = '''SCRIPT_VERSION = "{ver}"
import json, os, sys
if "--check-config" in sys.argv:
    cfg = sys.argv[sys.argv.index("--config") + 1]
    try:
        d = json.load(open(cfg))
        assert "mqtt" in d, "no mqtt section"
    except Exception as e:
        print("INVALID config " + str(e)); sys.exit(2)
    print("OK config=" + cfg + " stub"); sys.exit(0)
'''
HEALTH = '''import os, sys
# test double for tools/healthcheck.py: healthy unless the file named by $FAKE_HEALTH_FAIL_FOR holds the expected version
want = sys.argv[sys.argv.index("--expect-version") + 1] if "--expect-version" in sys.argv else ""
bad = open(os.environ["FAKE_HEALTH_FAIL_FOR"]).read().split() if os.environ.get("FAKE_HEALTH_FAIL_FOR") and os.path.exists(os.environ["FAKE_HEALTH_FAIL_FOR"]) else []
print("healthcheck: " + ("FAIL" if want in bad else "OK") + " - fake"); sys.exit(1 if want in bad else 0)
'''


def sh(cmd, env=None, check=True, cwd=None, **kw):
    e = dict(os.environ, UNIPI_MIN_FREE_MB="20", UNIPI_VENV_FLAGS="--without-pip", **(env or {}))     # the dev box /tmp is a small tmpfs
    r = subprocess.run(cmd, capture_output=True, text=True, env=e, cwd=cwd, **kw)
    if check and r.returncode != 0:
        raise AssertionError(f"{cmd} -> {r.returncode}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")
    return r


def git(repo, *a):
    return sh(["git", "-C", str(repo), "-c", "user.email=t@example.invalid", "-c", "user.name=t", *a])


@pytest.fixture
def src(tmp_path):
    """A tiny git repo shaped like this one, with tags v1 and v2 (v2 changes the script and requirements stay equal)."""
    r = tmp_path / "src"
    (r / "tools").mkdir(parents=True); (r / "systemd").mkdir(); (r / "web").mkdir(); (r / "unipi_core").mkdir()
    for f in ("bootstrap.sh", "lib-release.sh", "site_bundle.py", "site-bundle.sh", "deploy.sh", "deploy-release.sh", "rollback.sh"):
        (r / "tools" / f).write_text((TOOLS / f).read_text()); (r / "tools" / f).chmod(0o755)
    (r / "tools" / "healthcheck.py").write_text(HEALTH)
    (r / "systemd" / "hass-unipi.service").write_text((ROOT / "systemd" / "hass-unipi.service").read_text())
    (r / "web" / "index.html").write_text("<html></html>"); (r / "unipi_core" / "__init__.py").write_text("")
    (r / "requirements.txt").write_text("# no dependencies in the test release\n")
    (r / "hass-unipi.py").write_text(STUB.format(ver="1.0.0"))
    git(r, "init", "-q", "-b", "main"); git(r, "add", "-A"); git(r, "commit", "-qm", "v1"); git(r, "tag", "v1")
    (r / "hass-unipi.py").write_text(STUB.format(ver="2.0.0")); git(r, "commit", "-qam", "v2"); git(r, "tag", "v2")
    (r / "hass-unipi.py").write_text(STUB.format(ver="3.0.0-bad")); git(r, "commit", "-qam", "v3 (will be unhealthy)"); git(r, "tag", "v3")
    git(r, "checkout", "-q", "v1")
    return r


@pytest.fixture
def site(tmp_path):
    """A site bundle exported from a made-up classic runtime dir."""
    rt = tmp_path / "oldbox"; rt.mkdir()
    (rt / "config.json").write_text(json.dumps({"mqtt": {"broker": "mqtt.invalid", "password": "s3cret-pw"}}))  # pii-ok (fake test data)
    (rt / "local_rules.json").write_text("[]"); (rt / ".device_name").write_text("Neuron_TEST_1")
    ev = tmp_path / "oldbox-root" / "etc" / "evok"; ev.mkdir(parents=True); (ev / "config.yaml").write_text("comm_channels: {}\n")
    out = tmp_path / "t.site.tgz"
    sh(["python3", str(TOOLS / "site_bundle.py"), "export", "--out", str(out), "--prefix", str(tmp_path / "oldbox-root"), "--legacy-runtime", str(rt)])
    return out


def boot(src, prefix, site=None, *extra, check=True, tag=None):
    cmd = [str(src / "tools" / "bootstrap.sh"), "--prefix", str(prefix), "--yes", "--source-repo", str(src)]
    if site:
        cmd += ["--site", str(site)]
    if tag:
        cmd += ["--tag", tag]
    return sh(cmd + list(extra), check=check)


def snapshot(prefix):
    out = {}
    for p in sorted(Path(prefix).rglob("*")):
        rel = str(p.relative_to(prefix))
        if p.is_symlink():
            out[rel] = ("link", os.readlink(p))
        elif p.is_file():
            out[rel] = ("file", hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns, stat.S_IMODE(p.stat().st_mode))
        else:
            out[rel] = ("dir",)
    return out


# ---- site bundle ----------------------------------------------------------------------------------------------------------
def test_site_bundle_roundtrip_is_private_and_verified(site, tmp_path):
    assert stat.S_IMODE(site.stat().st_mode) == 0o600
    listing = sh(["python3", str(TOOLS / "site_bundle.py"), "list", str(site)]).stdout
    assert "all checksums OK" in listing and "etc-unipi-ha/config.json" in listing and "etc-evok/config.yaml" in listing
    dst = tmp_path / "new"
    sh(["python3", str(TOOLS / "site_bundle.py"), "import", str(site), "--prefix", str(dst)])
    cfg = dst / "etc/unipi-ha/config.json"
    assert json.loads(cfg.read_text())["mqtt"]["password"] == "s3cret-pw" and stat.S_IMODE(cfg.stat().st_mode) == 0o600
    assert (dst / "var/lib/unipi-ha/.device_name").read_text() == "Neuron_TEST_1"


def test_site_import_never_overwrites_without_force_and_keeps_the_old_file(site, tmp_path):
    dst = tmp_path / "new"; (dst / "etc/unipi-ha").mkdir(parents=True)
    (dst / "etc/unipi-ha/config.json").write_text('{"mine": true}')
    r = sh(["python3", str(TOOLS / "site_bundle.py"), "import", str(site), "--prefix", str(dst)], check=False)
    assert r.returncode == 2 and "would overwrite" in r.stderr and (dst / "etc/unipi-ha/config.json").read_text() == '{"mine": true}'
    sh(["python3", str(TOOLS / "site_bundle.py"), "import", str(site), "--prefix", str(dst), "--force"])
    assert [p.read_text() for p in (dst / "etc/unipi-ha").glob("config.json.pre-import-*")] == ['{"mine": true}']


def test_a_damaged_or_tampered_bundle_is_refused(site, tmp_path):
    import tarfile, io
    bad = tmp_path / "bad.site.tgz"
    with tarfile.open(site) as src_tf, tarfile.open(bad, "w:gz") as out:
        for ti in src_tf.getmembers():
            data = src_tf.extractfile(ti).read()
            if ti.name == "etc-unipi-ha/config.json":
                data = data.replace(b"s3cret", b"hacked")
            ti.size = len(data); out.addfile(ti, io.BytesIO(data))
    r = sh(["python3", str(TOOLS / "site_bundle.py"), "import", str(bad), "--prefix", str(tmp_path / "x")], check=False)
    assert r.returncode == 2 and "CHECKSUM MISMATCH" in r.stderr and not (tmp_path / "x").exists()
    r = sh(["python3", str(TOOLS / "site_bundle.py"), "list", str(tmp_path / "missing.tgz")], check=False)
    assert r.returncode == 2


def test_bundle_export_refuses_to_run_without_any_config(tmp_path):
    r = sh(["python3", str(TOOLS / "site_bundle.py"), "export", "--prefix", str(tmp_path / "e"), "--legacy-runtime", str(tmp_path / "none")], check=False)
    assert r.returncode == 2 and "nothing to export" in r.stderr


# ---- bootstrap ---------------------------------------------------------------------------------------------------------------
def test_dry_run_changes_nothing(src, site, tmp_path):
    p = tmp_path / "root"
    r = boot(src, p, site, "--dry-run")
    assert "WOULD" in r.stdout and "dry run: nothing was changed" in r.stdout
    assert not p.exists() or snapshot(p) == {}


def test_bootstrap_builds_the_whole_layout(src, site, tmp_path):
    p = tmp_path / "root"
    r = boot(src, p, site)
    assert (p / "opt/unipi-ha/current").is_symlink() and os.readlink(p / "opt/unipi-ha/current") == "releases/v1"
    assert (p / "opt/unipi-ha/releases/v1/VERSION").read_text().strip() == "v1"
    assert (p / "opt/unipi-ha/releases/v1/.venv/bin/python").exists()                       # shared venv reachable through the release
    assert not (p / "opt/unipi-ha/releases/v1/docs").exists()                                # only the runtime paths are released
    assert stat.S_IMODE((p / "etc/unipi-ha/config.json").stat().st_mode) == 0o600
    assert (p / "var/lib/unipi-ha/.device_name").read_text() == "Neuron_TEST_1"
    assert (p / "etc/evok/config.yaml").exists()
    assert (p / "etc/systemd/system/hass-unipi.service").read_text() == (ROOT / "systemd/hass-unipi.service").read_text()
    assert "OK config=" in r.stdout and "release: v1 (bridge 1.0.0)" in r.stdout
    state = (p / "var/lib/unipi-ha/.bootstrap-state").read_text()
    assert "release=v1" in state and "done.release=1" in state and "done.site=1" in state


def test_second_run_changes_nothing_at_all(src, site, tmp_path):
    p = tmp_path / "root"
    boot(src, p, site)
    before = snapshot(p)
    r = boot(src, p, site)
    assert snapshot(p) == before                                                               # tree, contents, mtimes, modes
    assert "changes: none (already up to date)" in r.stdout


def test_a_second_tag_gets_its_own_release_and_shares_the_venv(src, site, tmp_path):
    p = tmp_path / "root"
    boot(src, p, site)
    boot(src, p, site, tag="v2")
    assert os.readlink(p / "opt/unipi-ha/current") == "releases/v2" and (p / "opt/unipi-ha/previous").read_text().strip() == "v1"
    assert len(list((p / "opt/unipi-ha/venvs").iterdir())) == 1                                # same requirements -> one venv
    assert (p / "opt/unipi-ha/releases/v1/hass-unipi.py").read_text() != (p / "opt/unipi-ha/releases/v2/hass-unipi.py").read_text()


def test_bootstrap_refuses_bad_input_before_changing_anything(src, site, tmp_path):
    p = tmp_path / "root"
    assert boot(src, p, site, check=False, tag="v9").returncode == 2                           # unknown tag
    assert boot(src, p, tmp_path / "nope.site.tgz", check=False).returncode == 2               # missing bundle
    r = boot(src, p, site, "--harden-ssh", check=False)
    assert r.returncode == 2 and "key-login-verified" in r.stderr
    assert not p.exists() or snapshot(p) == {}


def test_an_invalid_configuration_stops_the_bootstrap(src, tmp_path):
    rt = tmp_path / "old"; rt.mkdir(); (rt / "config.json").write_text('{"no": "mqtt"}')
    bad = tmp_path / "bad.site.tgz"
    sh(["python3", str(TOOLS / "site_bundle.py"), "export", "--out", str(bad), "--prefix", str(tmp_path / "nr"), "--legacy-runtime", str(rt)])
    r = boot(src, tmp_path / "root", bad, check=False)
    assert r.returncode == 1 and "does not pass --check-config" in r.stderr


def test_without_a_site_bundle_it_warns_and_does_not_invent_a_config(src, tmp_path):
    p = tmp_path / "root"
    r = boot(src, p)
    assert "no --site bundle" in r.stderr and not (p / "etc/unipi-ha/config.json").exists()
    assert os.readlink(p / "opt/unipi-ha/current") == "releases/v1"                            # the code is installed anyway


def test_evok_config_from_the_bundle_is_applied_and_the_original_kept(src, site, tmp_path):
    p = tmp_path / "root"; (p / "etc/evok").mkdir(parents=True); (p / "etc/evok/config.yaml").write_text("original: yes\n")
    boot(src, p, site)
    assert (p / "etc/evok/config.yaml").read_text() == "comm_channels: {}\n"
    assert (p / "etc/evok/config.yaml.bootstrap-orig").read_text() == "original: yes\n"


def test_ssh_key_is_added_once(src, site, tmp_path):
    key = tmp_path / "id.pub"; key.write_text("ssh-ed25519 AAAATESTKEY test@example.invalid\n")
    p = tmp_path / "root"
    boot(src, p, site, "--authorized-key", str(key)); boot(src, p, site, "--authorized-key", str(key))
    ak = p / "home/unipi/.ssh/authorized_keys"
    assert ak.read_text().count("AAAATESTKEY") == 1 and stat.S_IMODE(ak.stat().st_mode) == 0o600


# ---- release-layout deploy + rollback ------------------------------------------------------------------------------------------
@pytest.fixture
def box(src, site, tmp_path):
    p = tmp_path / "root"
    boot(src, p, site)
    calls = tmp_path / "systemctl.log"
    fake = tmp_path / "fake-systemctl"
    fake.write_text(f'#!/usr/bin/env bash\necho "$@" >> {calls}\nexit 0\n'); fake.chmod(0o755)
    fail = tmp_path / "fail-versions"
    env = {"UNIPI_PREFIX": str(p), "UNIPI_SYSTEMCTL": str(fake), "UNIPI_HEALTHCHECK": f"python3 {src}/tools/healthcheck.py",
           "UNIPI_BACKUPS": str(tmp_path / "backups"), "FAKE_HEALTH_FAIL_FOR": str(fail)}
    return {"p": p, "env": env, "calls": calls, "fail": fail, "src": src}


def deploy(b, tag, *extra, check=True):
    return sh([str(b["src"] / "tools" / "deploy.sh"), tag, *extra], env=b["env"], check=check)


def current(b):
    return os.readlink(b["p"] / "opt/unipi-ha/current")


def test_deploy_flips_restarts_and_keeps_the_old_release(box):
    r = deploy(box, "v2")
    assert "SUCCESS" in r.stdout and current(box) == "releases/v2"
    assert "restart hass-unipi" in box["calls"].read_text()
    assert (box["p"] / "opt/unipi-ha/releases/v1").is_dir()                                    # rollback target stays on disk
    assert list((box["p"].parent / "backups").glob("*pre-v2.site.tgz"))                        # config safety copy


def test_an_unhealthy_release_flips_back_automatically(box):
    box["fail"].write_text("3.0.0-bad\n")
    r = deploy(box, "v3", check=False)
    assert r.returncode == 1 and "HEALTH CHECK FAILED" in r.stdout and "rolled back to" in r.stdout
    assert current(box) == "releases/v1"
    assert box["calls"].read_text().count("restart hass-unipi") == 2                          # once for v3, once back


def test_a_release_that_cannot_start_with_the_live_config_is_never_activated(box):
    (box["p"] / "etc/unipi-ha/config.json").write_text('{"broken": true}')
    r = deploy(box, "v2", check=False)
    assert r.returncode == 1 and "PREFLIGHT FAILED" in r.stderr and current(box) == "releases/v1"
    assert "restart" not in (box["calls"].read_text() if box["calls"].exists() else "")


def test_rollback_previous_and_to_a_tag_are_a_flip(box):
    deploy(box, "v2")
    r = sh([str(box["src"] / "tools" / "rollback.sh"), "previous"], env=box["env"])
    assert current(box) == "releases/v1"
    sh([str(box["src"] / "tools" / "rollback.sh"), "v2"], env=box["env"])
    assert current(box) == "releases/v2"
    # nothing recorded -> explicit error
    (box["p"] / "opt/unipi-ha/previous").unlink()
    assert sh([str(box["src"] / "tools" / "rollback.sh"), "previous"], env=box["env"], check=False).returncode == 2


def test_deploy_dry_run_and_unknown_tag_change_nothing(box):
    snap = snapshot(box["p"])
    assert "dry-run" in deploy(box, "v2", "--dry-run").stdout and snapshot(box["p"]) == snap
    assert deploy(box, "v9", check=False).returncode == 2 and snapshot(box["p"]) == snap


def test_old_releases_are_pruned_but_current_and_previous_survive(box, src):
    git(src, "checkout", "-q", "main")
    for i in range(4, 11):
        (src / "hass-unipi.py").write_text(STUB.format(ver=f"{i}.0.0")); git(src, "commit", "-qam", f"v{i}"); git(src, "tag", f"v{i}")
        git(src, "checkout", "-q", "main")
    # tools in the release dir come from the tag, so the deploy script of the *repo checkout* is used:
    for i in range(4, 11):
        deploy(box, f"v{i}")
        os.utime(box["p"] / f"opt/unipi-ha/releases/v{i}")                                     # distinct, increasing mtimes
    left = sorted(p.name for p in (box["p"] / "opt/unipi-ha/releases").iterdir())
    assert "v10" in left and "v9" in left and len(left) <= 6


def test_classic_mode_on_the_s103_is_untouched(tmp_path):
    """deploy.sh must only switch to release mode when current is a symlink; otherwise it runs the legacy code path."""
    text = (TOOLS / "deploy.sh").read_text()
    assert 'if [ -L "${UNIPI_PREFIX:-}/opt/unipi-ha/current" ]; then exec' in text
    assert (TOOLS / "deploy.sh").read_text().count("CODE_FILES=") == 1


def test_prune_never_removes_current_or_previous_even_when_they_are_old(tmp_path):
    opt = tmp_path / "opt"
    for i in range(1, 9):
        d = opt / "releases" / f"r{i}"; d.mkdir(parents=True); (d / "VERSION").write_text(f"r{i}")
        os.utime(d, (1_000_000 + i * 100, 1_000_000 + i * 100))                              # r8 newest, r1 oldest
    (opt / "venvs/used").mkdir(parents=True); (opt / "venvs/unused").mkdir()
    (opt / "releases/r8/.venv").symlink_to("../../venvs/used")
    os.symlink("releases/r1", opt / "current"); (opt / "previous").write_text("r2\n")       # rolled back to the OLDEST release
    sh(["bash", "-c", f'. {TOOLS}/lib-release.sh; release_prune {opt} 3'])
    assert sorted(p.name for p in (opt / "releases").iterdir()) == ["r1", "r2", "r6", "r7", "r8"]
    assert sorted(p.name for p in (opt / "venvs").iterdir()) == ["used"]                    # the unused venv is removed, the linked one stays
