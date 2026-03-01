import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "helpers"))
from execute import execute_runs

# ── paths ─────────────────────────────────────────────────────────────────────
DIR = Path(__file__).parent.resolve()
NIS = DIR / "../impl/nis"

_E = DIR / "samples/eclog"
_W = DIR / "samples/wikipedia"
_D = DIR / "samples/digenetica"

# ── runs ──────────────────────────────────────────────────────────────────────
RUNS = {
    "eclog-query": {
        "skip": False,
        "mode": "query",
        "data": _E / "ECOM-LOG.dat",
        "work": [
            _E / "ECOM-LOG.Q-rnd_qcnt10000-BASE.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-LOW.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-HIGH.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-SHORT.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-LONG.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-FEW.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-MANY.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-RND.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-SELECTIVE+BROAD.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-SELECTIVEWIDE+BROADNARROW.qry",
            _E / "ECOM-LOG.Q-rnd_qcnt10000-HOT+COLD.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.eclog.json"}
        ],
    },
    "eclog-query-genQ": {
        "skip": False,
        "mode": "query",
        "data": _E / "ECOM-LOG.dat",
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.eclog.json"}
        ],
    },
    "wikipedia-query": {
        "skip": False,
        "mode": "query",
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "work": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-BASE.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-LOW.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-HIGH.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SHORT.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-LONG.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-FEW.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-MANY.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-RND.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SELECTIVE+BROAD.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-SELECTIVEWIDE+BROADNARROW.qry",
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).Q-rnd_qcnt10000-HOT+COLD.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.wikipedia.json"}
        ],
    },
    "wikipedia-query-genQ": {
        "skip": False,
        "mode": "query",
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.wikipedia.json"}
        ],
    },
    "digenetica-query": {
        "skip": False,
        "mode": "query",
        "data": _D / "DIGINETICA-sessions_withProducts.dat",
        "work": [
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-BASE.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-LOW.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-HIGH.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-SHORT.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-LONG.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-FEW.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-MANY.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-RND.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-SELECTIVE+BROAD.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-SELECTIVEWIDE+BROADNARROW.qry",
            _D / "DIGINETICA-sessions_withProducts.Q-rnd_qcnt10000-HOT+COLD.qry",
        ],
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.digenetica.json"}
        ],
    },
    "digenetica-query-genQ": {
        "skip": False,
        "mode": "query",
        "data": _D / "DIGINETICA-sessions_withProducts.dat",
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.digenetica.json"}
        ],
    },
    "eclog-delete": {
        "skip": False,
        "mode": "delete",
        "data": _E / "ECOM-LOG.dat",
        "work": [
            _E / "ECOM-LOG.randomIDs-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/extract_relation_ids.py",
            "args": [_E / "ECOM-LOG.dat", "1"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.eclog.json"}
        ],
    },
    "wikipedia-delete": {
        "skip": False,
        "mode": "delete",
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "work": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).randomIDs-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/extract_relation_ids.py",
            "args": [_W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat", "1"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.wikipedia.json"}
        ],
    },
    "digenetica-delete": {
        "skip": False,
        "mode": "delete",
        "data": _D / "DIGINETICA-sessions_withProducts.dat",
        "work": [
            _D / "DIGINETICA-sessions_withProducts.randomIDs-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/extract_relation_ids.py",
            "args": [_D / "DIGINETICA-sessions_withProducts.dat", "1"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.digenetica.json"}
        ],
    },
    "eclog-update": {
        "skip": False,
        "mode": "update",
        "data": _E / "ECOM-LOG.part1-99percent.dat",
        "work": [
            _E / "ECOM-LOG.part2-1percent.dat"
        ],
        "pre_script": {
            "file": DIR / "helpers/split_relation.py",
            "args": [_E / "ECOM-LOG.dat", "99"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.eclog.json"}
        ],
    },
    "wikipedia-update": {
        "skip": False,
        "mode": "update",
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).part1-99percent.dat",
        "work": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).part2-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/split_relation.py",
            "args": [_W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat", "99"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.wikipedia.json"}
        ],
    },
    "digenetica-update": {
        "skip": False,
        "mode": "update",
        "data": _D / "DIGINETICA-sessions_withProducts.part1-99percent.dat",
        "work": [
            _D / "DIGINETICA-sessions_withProducts.part2-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/split_relation.py",
            "args": [_D / "DIGINETICA-sessions_withProducts.dat", "99"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.digenetica.json"}
        ],
    },
    "eclog-softdelete": {
        "skip": False,
        "mode": "softdelete",
        "data": _E / "ECOM-LOG.dat",
        "work": [
            _E / "ECOM-LOG.randomIDs-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/extract_relation_ids.py",
            "args": [_E / "ECOM-LOG.dat", "1"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.eclog.json"}
        ],
    },
    "wikipedia-softdelete": {
        "skip": False,
        "mode": "softdelete",
        "data": _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat",
        "work": [
            _W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).randomIDs-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/extract_relation_ids.py",
            "args": [_W / "WIKIPEDIA-100K+_random-articles-all-revisions_[2020-2024).dat", "1"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.wikipedia.json"}
        ],
    },
    "digenetica-softdelete": {
        "skip": False,
        "mode": "softdelete",
        "data": _D / "DIGINETICA-sessions_withProducts.dat",
        "work": [
            _D / "DIGINETICA-sessions_withProducts.randomIDs-1percent.dat",
        ],
        "pre_script": {
            "file": DIR / "helpers/extract_relation_ids.py",
            "args": [_D / "DIGINETICA-sessions_withProducts.dat", "1"]
        },
        "configs": [
            {"file": DIR / "idxcfg/SynthDex-project.digenetica.json"}
        ],
    },

}

execute_runs(RUNS, NIS, log_path=DIR / "exps.log")
