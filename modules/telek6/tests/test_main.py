"""Self-check: config, manifest patching and the deploy/forwarding paths against a fake kubectl,
then live against a telek6 server started from the sibling checkout.

TELEK6_APP=/path/to/telek6/app.py python -m modules.telek6.tests.test_main
The live half needs k6 on PATH and skips (exit 0, says so) when TELEK6_APP is unset.
"""
import os
import re
import stat
import subprocess
import sys
import tempfile
import threading
import time

import yaml

from modules.telek6.main import Telek6SmaModule, Telek6Forwarding

FAKE_KUBECTL = '''#!/bin/sh
echo "$@" >> "$FAKE_LOG"
case "$*" in
  *"svc telek6"*) echo 31234 ;;
  *"config view"*) echo https://10.0.0.5:6443 ;;
esac
'''

SCRIPT = """
const plan = open(__ENV.PLAN_PATH).trim();
export const options = { vus: 1, iterations: 3 };
export default function () { console.log(JSON.stringify({seq: __ITER, plan})); }
export function handleSummary(d) { return { stdout: JSON.stringify({kind: 'summary', n: d.metrics.iterations.values.count}) + '\\n' }; }
"""

URL = "http://127.0.0.1:1"


def fake_kubectl() -> str:
    """Put a recording kubectl first on PATH; returns the call log path."""
    d = tempfile.mkdtemp()
    fake = os.path.join(d, "kubectl")
    open(fake, "w").write(FAKE_KUBECTL); os.chmod(fake, stat.S_IRWXU)
    os.environ["PATH"] = d + os.pathsep + os.environ["PATH"]
    os.environ["FAKE_LOG"] = os.path.join(d, "calls.log")
    return os.environ["FAKE_LOG"]


def applied_manifest(calls: str) -> list:
    path = re.findall(r"apply -f (\S+)", calls)[-1]
    return list(yaml.safe_load_all(open(path)))


def main() -> int:
    log_path = fake_kubectl()
    calls = lambda: open(log_path).read() if os.path.exists(log_path) else ""

    for bad in ({"deploy": True}, {"url": URL, "nope": 1}, {"url": URL, "deploy": "/no/such.yaml"}):
        try:
            Telek6SmaModule(bad); raise AssertionError(bad)
        except (ValueError, FileNotFoundError):
            pass
    assert Telek6SmaModule({"port_forward": "nodeport"}).config.port_forward == Telek6Forwarding.NODEPORT
    assert Telek6SmaModule({"port_forward": True}).config.port_forward == Telek6Forwarding.PROXY
    print("ok  config: url required unless port_forward derives it; unknown keys and missing manifests refused")

    mod = Telek6SmaModule({"url": URL, "deploy": True, "kube_context": "kind-x", "namespace": "lg", "undeploy": True,
                           "deploy_timeout_s": 7, "nodeSelector": ["kubernetes.io/arch=amd64", "role="],
                           "image": "reg.local/telek6:dev", "imagePullSecrets": "regcred"})
    mod.deploy()
    assert re.search(r"--context kind-x create namespace lg\n"
                     r"--context kind-x -n lg apply -f \S+\n"
                     r"--context kind-x -n lg wait deployment/telek6 --for=condition=Available --timeout=7s\n$", calls()), calls()
    dep, svc = applied_manifest(calls())
    pod = dep["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"kubernetes.io/arch": "amd64", "role": ""}, pod["nodeSelector"]
    assert pod["imagePullSecrets"] == [{"name": "regcred"}]
    (c,) = pod["containers"]
    assert c["image"] == "reg.local/telek6:dev" and c["ports"] and c["readinessProbe"], "patch must keep the rest of the container"
    assert "namespace" not in dep["metadata"] and "nodePort" not in svc["spec"]["ports"][0], "manifest carries no placement opinions"
    mod.onTeardown()
    assert calls().endswith("--context kind-x -n lg delete -f %s --ignore-not-found\n" % mod.manifest), calls()
    print("ok  deploy: true patches the bundled manifest (nodeSelector, image, imagePullSecrets) and applies it; undeploy deletes it")

    plain = Telek6SmaModule({"url": URL, "deploy": True})
    plain.deploy()
    pod = applied_manifest(calls())[0]["spec"]["template"]["spec"]
    assert "nodeSelector" not in pod and "imagePullSecrets" not in pod
    assert pod["containers"][0]["image"] == "ghcr.io/distributed-experiment-toolbox/telek6:0.3.1", "prepared runs need telek6 >= 0.3.0"
    assert pod["volumes"][0]["emptyDir"]["sizeLimit"], "the long-lived pod's run data is bounded"
    n_delete = calls().count("delete")
    plain.onTeardown()
    assert calls().count("delete") == n_delete, "undeploy: false must keep the pod"
    print("ok  without patches the bundled manifest is applied as is; undeploy only when asked")

    own = tempfile.mktemp(suffix=".yaml")
    yaml.safe_dump_all(list(yaml.safe_load_all(open(str(mod.prepare_manifest())))), open(own, "w"))
    Telek6SmaModule({"url": URL, "deploy": own, "image": "x:1"}).deploy()
    assert applied_manifest(calls())[0]["spec"]["template"]["spec"]["containers"][0]["image"] == "x:1"
    print("ok  deploy: <path> takes a project's own manifest through the same patches")

    np = Telek6SmaModule({"port_forward": "nodeport", "nodeIP": "1.2.3.4"})
    assert np.nodeport_url() == "http://1.2.3.4:31234"
    assert Telek6SmaModule({"port_forward": "nodeport"}).nodeport_url() == "http://10.0.0.5:31234"
    print("ok  nodeport: nodeIP (or the kubeconfig server host) + the Service's assigned nodePort")

    app = os.environ.get("TELEK6_APP")
    if not app:
        print("SKIP  set TELEK6_APP=/path/to/telek6/app.py for the live check")
        return 0
    port, data = 5198, tempfile.mkdtemp()
    srv = subprocess.Popen([sys.executable, app], env={**os.environ, "TELEK6_HTTP_PORT": str(port), "TELEK6_DATA": data},
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        mod = Telek6SmaModule({"url": f"http://127.0.0.1:{port}", "timeout_s": 30, "deploy": True})
        mod.onSetup()   # applies (fake kubectl), then health-checks with retries while the server boots
        assert re.search(r"-n telek6 wait deployment/telek6 --for=condition=Available --timeout=300s\n$", calls()), calls()
        print("ok  onSetup deploys, then waits for a healthy server")
        work = tempfile.mkdtemp()
        script = os.path.join(work, "s.js"); plan = os.path.join(work, "plan.ndjson")
        open(script, "w").write(SCRIPT); open(plan, "w").write("p1\n")
        out = os.path.join(work, "out")
        meta = mod.trigger(threading.Event(), script_path=script, files={"plan.ndjson": plan},
                           env={"PLAN_PATH": "plan.ndjson"}, download_dir=out)
        assert meta["k6_status"] == "complete" and meta["exit_code"] == 0, meta
        assert "console.ndjson" in meta["downloaded_files"] and meta["run_dir"] == out, meta
        recs = [l for l in open(os.path.join(out, "console.ndjson")) if l.startswith("{")]
        assert len(recs) == 3 and '"plan":"p1"' in recs[0], recs
        print("ok  trigger runs a script with a file and env, downloads into download_dir")

        long_script = SCRIPT.replace("iterations: 3", "iterations: 100000")
        open(script, "w").write(long_script)
        cancel = threading.Event()
        threading.Timer(1.0, cancel.set).start()
        meta = mod.trigger(cancel, script_path=script, files={"plan.ndjson": plan}, env={"PLAN_PATH": "plan.ndjson"})
        assert meta["k6_status"] == "cancelled" and os.path.isdir(meta["run_dir"]), meta
        assert any(l.startswith("{") for l in open(os.path.join(meta["run_dir"], "stdout.ndjson"))), "summary after stop"
        print("ok  cancel stops the run; partial data and the summary are still downloaded")

        class R: location = tempfile.mkdtemp()
        mod.onReport(R())
        assert os.path.isdir(os.path.join(R.location, "telek6")) and not os.path.exists(meta["run_dir"])
        print("ok  onReport moves a temp download under the report")

        # prepared runs against the real server, driven through the module's own hooks
        open(script, "w").write(SCRIPT)
        for start_at in ("treatment", "left_window"):
            pm = Telek6SmaModule({"url": f"http://127.0.0.1:{port}", "timeout_s": 30, "start_at": start_at})
            pm.onSetup()   # healthy, then reset
            out = os.path.join(work, f"prepared-{start_at}")
            kw = dict(script_path=script, files={"plan.ndjson": plan}, env={"PLAN_PATH": "plan.ndjson"}, download_dir=out)
            pm.onRunParameters(**kw)
            pm.onRunStart()
            token = pm._run.token
            assert pm.client.status(token)["ready"], "onRunStart returns once every VU is initialised"
            pm.onLeftWindowStart()
            assert (pm.client.status(token)["resumed_at"] is not None) == (start_at == "left_window")
            meta = pm.trigger(threading.Event(), **kw)
            assert meta["token"] == token and meta["k6_status"] == "complete" and meta["start_at"] == start_at, meta
            assert meta["resumed_at"] >= meta["started_at"] and meta["prepare_s"] > 0, meta
            assert len([l for l in open(os.path.join(out, "console.ndjson")) if l.startswith("{")]) == 3
            pm.onRunEnd()
            assert pm.client.status(token)["status"] == "finished"
            print(f"ok  start_at: {start_at}: prepared until ready, resumed, waited on, downloaded")

        pm = Telek6SmaModule({"url": f"http://127.0.0.1:{port}", "timeout_s": 30, "start_at": "treatment"})
        pm.onSetup()
        pm.onRunParameters(script_path=script, files={"plan.ndjson": plan}, env={"PLAN_PATH": "plan.ndjson"})
        pm.onRunStart()
        token = pm._run.token
        pm.onRunEnd()   # the run never reached its trigger: the paused k6 must not survive it
        assert pm.client.status(token)["status"] == "finished", pm.client.status(token)
        print("ok  onRunEnd stops a prepared k6 run the trigger never used")

        bad = os.path.join(work, "bad.js")
        open(bad, "w").write("export const options = { vus: 5, iterations: 1 };\nexport default function () {}\n")
        pm.onRunParameters(script_path=bad)
        try:
            pm.onRunStart(); raise AssertionError("a script k6 refuses must fail the prepare")
        except RuntimeError as exc:
            assert "ended before it was ready" in str(exc), exc
        print("ok  a k6 run that exits during its start-up fails the prepare")
        print("PASS")
        return 0
    finally:
        srv.terminate()


if __name__ == "__main__":
    sys.exit(main())
