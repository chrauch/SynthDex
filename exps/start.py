import json
import random
import subprocess
from pathlib import Path

# ── paths ─────────────────────────────────────────────────────────────────────
DIR = Path(__file__).parent.resolve()
NIS = DIR / "../impl/nis"

_E = DIR / "samples/eclog"
_W = DIR / "samples/wikipedia"
_D = DIR / "samples/digenetica"

# ── runs ──────────────────────────────────────────────────────────────────────
RUNS = {
    "eclog-old": {
        "skip": True,
        "data": _E / "ECOM-LOG.dat",
        "qrys": [
            _E / "ECOM-LOG.dat_10K_elems1-extent0.1%.qry",
            _E / "ECOM-LOG.dat_10K_elems2-extent0.1%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.01%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.05%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.1%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.1%-elemid[select_0.1%-1%].qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.1%-elemid[select_1%-10%].qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.1%-elemid[select_10%-100%].qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.1%-elemid[select_less-than-0.1%].qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent0.5%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent1.0%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent5.0%.qry",
            _E / "ECOM-LOG.dat_10K_elems3-extent10.0%.qry",
            _E / "ECOM-LOG.dat_10K_elems4-extent0.1%.qry",
            _E / "ECOM-LOG.dat_10K_elems5-extent0.1%.qry",
            _E / "ECOM-LOG.dat_10K_elems7-extent0.1%.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.eclog.json"},
            {"file": DIR / "idxcfg/good.json"}
        ],
    },
    "eclog-new": {
        "skip": True,
        "data": _E / "ECOM-LOG.dat",
        "qrys": [
            _E / "ECOM-LOG.Q-rnd_qcnt10000-BASE.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-BROAD.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-BROADNARROW.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-FEW.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-HIGHFREQ.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-LONG.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-LOWFREQ.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-MANY.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-RND2.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-SELECTIVE.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-SELECTIVEWIDE.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-SHORT.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt9999-SELECTIVE+BROAD.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt9999-SELECTIVEWIDE+BROADNARROW.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.eclog.json"},
            {"file": DIR / "idxcfg/good.json"}
        ],
    },
    "eclog-new-temp": {
        "skip": True,
        "data": _E / "ECOM-LOG.dat",
        "qrys": [
            _E / "ECOM-LOG.Q-rnd_qcnt10000-COLD.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-HOT.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-HOT+COLD.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.eclog.json"},
            {"file": DIR / "idxcfg/good-temp.json"}
        ],
    },
    "eclog-new-genQ": {
        "skip": False,
        "data": _E / "ECOM-LOG.dat",
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.eclog.json"},
            {"file": DIR / "idxcfg/good.json"},
            {"file": DIR / "idxcfg/good-temp.json"}
        ],
    },
    "wikipedia-old": {
        "skip": True,
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "qrys": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems1-extent0.1%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems2-extent0.1%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.01%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.05%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.1%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.1%-elemid[select_0.1%-1%].qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.1%-elemid[select_1%-10%].qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.1%-elemid[select_10%-100%].qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.1%-elemid[select_less-than-0.1%].qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent0.5%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent1.0%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent5.0%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems3-extent10.0%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems4-extent0.1%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems5-extent0.1%.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat_10K_elems7-extent0.1%.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.wikipedia.json"},
            {"file": DIR / "idxcfg/good.json"},
        ],
    },
    "wikipedia-new": {
        "skip": True,
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "qrys": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-BASE.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-BROAD.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-BROADNARROW.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-FEW.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-HIGHFREQ.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-LONG.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-LOWFREQ.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-MANY.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-RND2.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SELECTIVE.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SELECTIVE+BROAD.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SELECTIVEWIDE.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SELECTIVEWIDE+BROADNARROW.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SHORT.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.wikipedia.json"},
            {"file": DIR / "idxcfg/good.json"},
        ],
    },
    "wikipedia-new-temp": {
        "skip": True,
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "qrys": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-COLD.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-HOT.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-HOT+COLD.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.wikipedia.json"},
            {"file": DIR / "idxcfg/good-temp.json"},
        ],
    },
    "wikipedia-new-genQ": {
        "skip": False,
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.wikipedia.json"},
            {"file": DIR / "idxcfg/good.json"},
            {"file": DIR / "idxcfg/good-temp.json"},
        ],
    },
    "digenetica-old": {
        "skip": True,
        "data": _D / "DIGINETICA-sessions_withProducts.dat",
        "qrys": [
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems1-extent0.1%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems2-extent0.1%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.01%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.05%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.1%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.1%-elemid[select_0.1%-1%].qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.1%-elemid[select_1%-5%].qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.1%-elemid[select_5%-10%].qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.1%-elemid[select_less-than-0.1%].qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent0.5%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent1.0%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent5.0%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems3-extent10.0%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems4-extent0.1%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems5-extent0.1%.qry",
            _D / "DIGINETICA-sessions_withProducts.dat_10K_elems7-extent0.1%.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.eclog.json"},
            {"file": DIR / "idxcfg/good.json"}
        ],
    },
    "digenetica-new-genQ": {
        "skip": False,
        "data": _D / "DIGINETICA-sessions_withProducts.dat",
        "configs": [
            {"file": DIR / "idxcfg/fastTIR-project.eclog.json"},
            {"file": DIR / "idxcfg/good.json"},
            {"file": DIR / "idxcfg/good-temp.json"}
        ],
    },

}

# ── main loop ─────────────────────────────────────────────────────────────────
for run_name, run in RUNS.items():

    if run.get("skip", False): continue

    print(f"\n{'='*60}")
    print(f"RUN: {run_name}")
    print(f"{'='*60}")

    data = run["data"]
    qrys = run.get("qrys", [])

    configs = []
    for cfg_entry in run["configs"]:
        cfg_file = cfg_entry["file"]
        limit    = cfg_entry.get("limit", None)
        items = json.loads(Path(cfg_file).read_text())
        lines = [json.dumps(item, separators=(',', ':')) for item in items]
        if limit is not None:
            random.shuffle(lines)
            lines = lines[:limit]
        configs.extend(lines)

    config_str = "|".join(configs)

    if len(qrys) == 0:
        cmd = [str(NIS), "query", config_str, str(data)]
    else:
        qry_str = "|".join(str(q) for q in qrys)
        cmd = [str(NIS), "query", config_str, str(data), qry_str]

    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd, text=True, capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"  [FAILED] exit code {result.returncode}")
