/******************************************************************************
 * Project:  SynthDex
 * Purpose:  Adaptive Ensemble Indexing for Temporal Information Retrieval via Learned Cost Models
 * Author:   Christian Rauch
 ******************************************************************************
 * Copyright (c) 2025 - 2026
 *
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

#include "controller.h"
#include "executionrunner.h"
#include "accuracyrunner.h"
#include "noiserunner.h"
#include "rankingrunner.h"
#include "comparisonrunner.h"
#include "../synthesis/tuningrunner.h"
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>
#include <sstream>
#include <filesystem>

using namespace std;


Controller::Controller(Cmd &cmd)
    : cmd(cmd)
{
    Log::level_output = Cfg::get<int>("log-level-output");
}


void Controller::start()
{
    if (!this->cmd.logs && !this->cmd.config && !this->cmd.help)
    {
        Log::w(0, "\n\nNeural Index Synthesis\n======================");
        Log::w(2, "Settings", Cfg::singleton->str());
        Log::w(2, "Threads", Cfg::get_threads());
    }

    if (this->cmd.help)
    {
        this->help();
        return;
    }

    if (this->cmd.clean != "")
    {
        this->clean();
        return;
    }

    if (this->cmd.analyze)
    {
        this->analyze();
        return;
    }

    if (this->cmd.setup)
    {
        this->setup();
        return;
    }

    if (this->cmd.learn)
    {
        this->learn();
        return;
    }

    if (this->cmd.convert)
    {
        this->convert();
        return;
    }

    if (this->cmd.synth)
    {
        this->synthesize();
        return;
    }

    if (this->cmd.score)
    {
        this->score();
        return;
    }

    if (this->cmd.accuracy)
    {
        this->accuracy_check();
        return;
    }

    if (this->cmd.noise)
    {
        this->noise_check();
        return;
    }

    if (this->cmd.ranking)
    {
        this->ranking_check();
        return;
    }

    if (this->cmd.tuning)
    {
        this->tuning_check();
        return;
    }

    if (this->cmd.compare)
    {
        this->compare();
        return;
    }

    if (this->cmd.logs)
    {
        this->logs();
        return;
    }

    if (this->cmd.seq)
    {
        this->sequence();
        return;
    }

    if (this->cmd.config)
    {
        this->config();
        return;
    }

    if (this->cmd.update)
    {
        this->update();
        return;
    }

    if (this->cmd.remove)
    {
        this->remove();
        return;
    }

    if (this->cmd.softdelete)
    {
        this->softdelete();
        return;
    }

    if (this->cmd.gen_O)
    {
        int o_num = Cfg::get<int>("o.num");
        Timer o_eta_timer;
        o_eta_timer.start();
        int total_evals = 0;

        for (int oi = 0; oi < o_num; oi++)
        {
            this->generate_O(oi);

            this->Q.clear();
            this->Q_patterns.clear();
    
            if (this->cmd.gen_Q)
            {
                this->generate_Q();

                if (this->cmd.query || this->cmd.predict)
                    total_evals += this->execute();
            }
            else if (!this->cmd.file_Q.empty()
                && (this->cmd.query || this->cmd.predict))
            {
                this->load_Q(this->cmd.file_Q);
                total_evals += this->execute();
            }

            if (o_num > 1)
            {
                double elapsed = o_eta_timer.stop();
                double avg = elapsed / (oi + 1);
                double remaining = avg * (o_num - oi - 1);
                int rem_min = (int)(remaining / 60);
                int rem_sec = (int)remaining % 60;
                double evals_per_min = (elapsed > 0)
                    ? (total_evals / elapsed) * 60.0 : 0.0;
                char epm_str[32];
                snprintf(epm_str, sizeof(epm_str), "%.1f", evals_per_min);
                Log::w(0, "ETA for all Os", to_string(rem_min) + "m " + to_string(rem_sec) + "s"
                    + " (" + to_string((int)elapsed) + "s elapsed, "
                    + to_string(o_num - oi - 1) + " remaining, "
                    + string(epm_str) + " evals/min)");
            }
        }
    }
    else
    {
        this->load_O();

        if (this->cmd.gen_Q)
        {
            this->generate_Q();

            if (this->cmd.query || this->cmd.predict) this->execute();
        }
        else
        {
            this->load_Q(this->cmd.file_Q);

            this->execute();
        }
    }
}


int Controller::execute(ComparisonMeasurements* comparison_measurements)
{
    ExecutionRunner runner(this->cmd.idxschema, this->cmd.query, this->cmd.predict,
        this->cmd.file_O, this->execution_O_file, this->cmd.file_Q,
        this->O, this->Q,
        this->Ostats, this->statscomp,
        this->cmd.groups, this->Q_patterns, comparison_measurements);

    if (this->cmd.query && !this->execution_O_file.empty())
    {
        this->O.clear();
        this->O.shrink_to_fit();
    }

    return runner.execute();
}


void Controller::load_O()
{
    Log::w(0, "Objects");
    this->O = Persistence::read_O_dat(this->cmd.file_O);
    this->execution_O_file = this->cmd.file_O;

    if (Cfg::get<bool>("o.reuse-ostats") 
        && Persistence::exists_O_stats_json(this->cmd.file_O))
    {
        Log::w(1, "Reusing OStats from file");
        this->Ostats = Persistence::read_O_stats_json(this->cmd.file_O);
        this->statscomp.Ostats = this->Ostats;
    }
    else
    {
        this->Ostats = this->statscomp.analyze_O(this->O, this->cmd.file_O);
        Persistence::write_O_stats(this->Ostats, 0);
    }
}


void Controller::compare()
{
    ComparisonRunner(*this).run();
}


void Controller::load_Q(const string& file)
{
    Log::w(0, "Queries");

    stringstream ss(file);
    string qfile;
    while (getline(ss, qfile, '|'))
    {
        if (qfile.empty()) continue;

        if (qfile[0] == '%')
        {
            // On-demand query generation: %TAG generates queries using current
            // config and caches the result; subsequent calls reuse the file.
            string raw_tag = qfile.substr(1);

            // Parse and strip any {override} blocks from the tag.
            auto [tag, workload_cfg] = parse_pattern_overrides(raw_tag);

            // Build hash suffix from workload_cfg (empty string when no CLI attrs).
            string ov_suffix;
            if (!workload_cfg.empty())
            {
                string canonical = overrides_canonical(workload_cfg);
                char buf[16];
                snprintf(buf, sizeof(buf), "%08x", fnv1a_32(canonical));
                ov_suffix = string("-ov-") + buf;
                Log::w(1, "Q overrides", string(buf) + " (from: " + canonical + ")");
            }

            Log::w(1, "Q on-demand generation (tag)",
                tag.empty() ? "(default)" : tag);

            int num_override  = Cfg::get<int>("q.gen.num");
            int expand_seed   = Cfg::get<int>("q.gen.expand.seed");
            int mutation_rate = Cfg::get<int>("q.gen.expand.mutation-rate-pct");
            string exp_tag    = expand_seed > 0
                ? "s" + to_string(expand_seed) + "m" + to_string(mutation_rate)
                : "";

            // Search for an existing cached file before generating.
            // When q.gen.num > 0, only accept a file whose qcnt matches
            // the override exactly; otherwise accept any qcnt.
            // When expand.seed > 0, exp_tag is embedded in the name before
            // the tag, so include it in both the suffix and exact_prefix.
            string data_dir = Cfg::get_out_dir() + "/data";
            string prefix = this->Ostats.name + ".Q-rnd_qcnt";
            string suffix = exp_tag + (tag.empty() ? "" : "-" + tag) + ov_suffix + ".qry";
            string exact_prefix = num_override > 0
                ? prefix + to_string(num_override)
                : "";
            string found_path;
            if (filesystem::exists(data_dir))
            {
                for (const auto& entry : filesystem::directory_iterator(data_dir))
                {
                    if (!entry.is_regular_file()) continue;
                    string fname = entry.path().filename().string();
                    if (fname.size() <= suffix.size()) continue;
                    if (fname.substr(fname.size() - suffix.size()) != suffix) continue;
                    bool prefix_ok = !exact_prefix.empty()
                        ? fname.rfind(exact_prefix, 0) == 0
                        : fname.rfind(prefix, 0) == 0;
                    if (prefix_ok)
                    {
                        found_path = entry.path().string();
                        break;
                    }
                }
            }

            if (!found_path.empty())
            {
                Log::w(1, "Q file already exists, reusing", found_path);
                auto Qx = Persistence::read_q_dat(found_path);
                // Recover the original CLI pattern stored in the .qry header (if present).
                // Fall back to raw_tag (from the CLI) for files that predate pattern storage.
                string pat = Persistence::read_q_raw_pattern(found_path);
                if (pat.empty()) pat = raw_tag + "_from_filename";
                for (auto& q : Qx)
                {
                    this->Q_patterns[get<0>(q)] = pat;
                    this->Q.push_back(move(q));
                }
            }
            else
            {
                // When expansion is active, generate only the seed-count of
                // queries first, then expand to the full target.
                int gen_count = (expand_seed > 0) ? expand_seed : num_override;
                QGen qgen(this->O, this->Ostats);
                if (!workload_cfg.empty()) qgen.set_workload_cfg(workload_cfg);
                auto raw_Q = qgen.construct_Q(tag, gen_count);
                if (expand_seed > 0)
                    qgen.expand_queries(raw_Q, num_override > 0 ? num_override : (int)raw_Q.size());
                if (!raw_Q.empty())
                {
                    auto tup = make_tuple(
                        "qcnt" + to_string(raw_Q.size()) + exp_tag + "-" + tag + ov_suffix,
                        move(raw_Q));
                    const string q_name = get<0>(tup);
                    string path = Persistence::get_Q_dat_path(this->Ostats, tup);
                    Log::w(1, "Writing Q file", path);
                    Persistence::write_Q_dat(tup, this->Ostats, workload_cfg, raw_tag);
                    this->Q_patterns[q_name] = raw_tag;
                    this->Q.push_back(move(tup));
                }
            }
        }
        else
        {
            auto Qx = Persistence::read_q_dat(qfile);
            string pat = Persistence::read_q_raw_pattern(qfile);
            for (auto& q : Qx)
            {
                this->Q_patterns[get<0>(q)] = pat;
                this->Q.push_back(move(q));
            }
        }
    }
}


void Controller::update()
{
    bool is_synth = (this->cmd.idxschema == "?");
    bool has_qry  = !this->cmd.file_Q.empty();

    if (is_synth && !has_qry)
        throw runtime_error("update with '?' requires <qry> for synthesis");
    if (!is_synth && has_qry)
        throw runtime_error("<qry> is only valid with synthesis mode ('?')");

    auto schema_str = is_synth ? this->synthesize_for_mutation() : this->cmd.idxschema;
    ExecutionRunner::update(this->cmd.file_O, this->cmd.file_O2, schema_str, this->statscomp,
        is_synth ? "synthetic" : "manual");
}


void Controller::remove()
{
    bool is_synth = (this->cmd.idxschema == "?");
    bool has_qry  = !this->cmd.file_Q.empty();

    if (is_synth && !has_qry)
        throw runtime_error("delete with '?' requires <qry> for synthesis");
    if (!is_synth && has_qry)
        throw runtime_error("<qry> is only valid with synthesis mode ('?')");

    auto schema_str = is_synth ? this->synthesize_for_mutation() : this->cmd.idxschema;
    ExecutionRunner::remove(this->cmd.file_O, this->cmd.file_O2, schema_str, this->statscomp,
        is_synth ? "synthetic" : "manual");
}


void Controller::softdelete()
{
    bool is_synth = (this->cmd.idxschema == "?");
    bool has_qry  = !this->cmd.file_Q.empty();

    if (is_synth && !has_qry)
        throw runtime_error("softdelete with '?' requires <qry> for synthesis");
    if (!is_synth && has_qry)
        throw runtime_error("<qry> is only valid with synthesis mode ('?')");

    auto schema_str = is_synth ? this->synthesize_for_mutation() : this->cmd.idxschema;
    ExecutionRunner::softdelete(this->cmd.file_O, this->cmd.file_O2, schema_str, this->statscomp,
        is_synth ? "synthetic" : "manual");
}


void Controller::analyze()
{
    this->O = Persistence::read_O_dat(this->cmd.file_O);

    if (Cfg::get<bool>("o.reuse-ostats")
        && Persistence::exists_O_stats_json(this->cmd.file_O))
    {
        Log::w(1, "Reusing OStats from file");
        this->Ostats = Persistence::read_O_stats_json(this->cmd.file_O);
    }
    else
    {
        this->Ostats = this->statscomp.analyze_O(this->O, this->cmd.file_O);
        Persistence::write_O_stats(this->Ostats, 0);
    }

    // O-only slice analysis when no Q is provided.
    if (this->cmd.file_Q.empty())
    {
        vector<RangeIRQuery> empty_Q;
        auto sliced = this->statscomp.analyze_Osliced(this->O, this->cmd.file_O, empty_Q);
        Persistence::write_O_stats_sliced(sliced);
    }

    if (!this->cmd.file_Q.empty())
    {
        // load_Q handles %BASE, pipe-separated files and on-demand generation;
        // requires this->O and this->Ostats to be set (done above).
        this->load_Q(this->cmd.file_Q);

        // Combined O+Q temporal slice analysis using the full flattened workload.
        {
            vector<RangeIRQuery> all_Q;
            for (const auto &[pattern, qs] : this->Q)
                all_Q.insert(all_Q.end(), qs.begin(), qs.end());

            auto sliced = this->statscomp.analyze_Osliced(this->O, this->cmd.file_O, all_Q);
            Persistence::write_O_stats_sliced(sliced);
        }

        for (const auto &[pattern, Q] : this->Q)
            this->statscomp.analyze_Q(Q);
        Persistence::write_Q_stats_csv(this->statscomp.Qstats);
    }
}


void Controller::generate_Q()
{
    Log::w(0, "Queries");

    this->Q_patterns.clear();

    QGen qgen(this->O, this->Ostats);
    string ov_suffix;
    unordered_map<string, unordered_map<string,string>> workload_cfg;
    if (!this->cmd.q_pattern.empty())
    {
        string clean_pattern;
        tie(clean_pattern, workload_cfg) = parse_pattern_overrides(this->cmd.q_pattern);
        if (!workload_cfg.empty())
        {
            string canonical = overrides_canonical(workload_cfg);
            char buf[16];
            snprintf(buf, sizeof(buf), "%08x", fnv1a_32(canonical));
            ov_suffix = string("-ov-") + buf;
            Log::w(1, "Q overrides", string(buf) + " (from: " + canonical + ")");
            qgen.set_workload_cfg(workload_cfg);
        }
        qgen.set_patterns({clean_pattern});
    }
    this->Q = qgen.construct_Q();

    // Embed the override hash in each tuple's name so files with different
    // overrides are stored under distinct filenames.
    if (!ov_suffix.empty())
    {
        for (auto& tup : this->Q)
            get<0>(tup) += ov_suffix;
    }

    if (!this->cmd.query && !this->cmd.predict && !this->cmd.synth)
        for (auto& Q : this->Q)
        {
            Persistence::write_Q_dat(Q, this->Ostats, workload_cfg, this->cmd.q_pattern);
            this->Q_patterns[get<0>(Q)] = this->cmd.q_pattern;
        }
}


void Controller::generate_O(int &i)
{
    Log::w(0, progress("Objects", i, Cfg::get<int>("o.num")));

    this->Ostats = OStatsGen().create(i);

    if (!this->cmd.gen_O || Cfg::get<bool>("out.detailed"))
        Persistence::write_O_stats(this->Ostats, i);

    if (!this->cmd.gen_O) return;

    this->O = OGen(this->Ostats).construct_O();

    Persistence::write_O_dat(this->O, this->Ostats);
    this->execution_O_file = Persistence::get_O_dat_path(this->Ostats);

    this->Ostats = this->statscomp.analyze_O(
        this->O, this->Ostats.name + "-generated");

    Persistence::write_O_stats(this->Ostats, i);
}


void Controller::learn()
{
    ProcessExec::python_setup(false);

    Log::w(0, "Learning");

    ProcessExec::python_run("Training",
        "./learning/trainer.py " + Cfg::config_path + " \"" + Cfg::get_out_dir() + "\"", 1);
}


void Controller::convert()
{
    StatsConvert().run();
}


string Controller::synthesize_for_mutation()
{
    this->load_O();
    this->load_Q(this->cmd.file_Q);

    auto prep = SynthesisRunner::prepare_banding(
        this->O, this->cmd.file_O, this->Q, this->statscomp, this->cmd.groups);

    // Banded path: try to produce a banded schema first.
    if (prep.has_banded_templates && !prep.batched_Q.empty())
    {
        SynthesisRunner synthesisrunner(
            prep.batched_Q, this->statscomp, prep.per_q_ostats,
            this->cmd.groups, prep.band_inner_templates);
        auto suggestions = synthesisrunner.run();

        if (!suggestions.empty())
        {
            bool has_bands = any_of(suggestions.begin(), suggestions.end(),
                [](const auto& p) { return any_of(p.second.begin(), p.second.end(),
                    [](const auto& s) { return !s.temporal_band.empty(); }); });

            if (has_bands)
            {
                auto br = SynthesisRunner::process_banded(
                    suggestions, prep.batched_Q, prep.all_slice_bounds, this->O.gend);
                if (!br.schemas.empty())
                    return IdxSchemaSerializer::to_json_banded_line(br.schemas);
            }
        }
    }

    // Non-banded path (or fallthrough when banding yielded no bands).
    {
        const vector<string>& groups = prep.has_banded_templates
            ? prep.non_banded_groups : this->cmd.groups;
        SynthesisRunner synthesisrunner(this->Q, this->statscomp, {}, groups);
        auto suggestions = synthesisrunner.run();

        if (suggestions.empty() || suggestions.begin()->second.empty())
            throw runtime_error("Synthesis produced no suggestions");

        auto selected = SynthesisRunner::select_size_aware(
            suggestions.begin()->second);
        return IdxSchemaSerializer::to_json_line(
            selected->idxschema);
    }
}


void Controller::synthesize()
{
    this->load_O();

    if (this->cmd.gen_Q)
        this->generate_Q();
    else
        this->load_Q(this->cmd.file_Q);

    // Banded synthesis: each Q workload is synthesized independently so that
    // temporal band boundaries are derived from that workload's queries only.
    // Template flags are Q-independent; capture them from the first call.
    bool has_banded_templates = false;
    bool has_non_banded_templates = false;
    vector<string> non_banded_groups;

    for (const auto& Qx : this->Q)
    {
        auto prep = SynthesisRunner::prepare_banding(
            this->O, this->cmd.file_O, { Qx }, this->statscomp, this->cmd.groups);

        if (!has_banded_templates && prep.has_banded_templates)
        {
            has_banded_templates = true;
            non_banded_groups = prep.non_banded_groups;
        }
        if (prep.has_non_banded_templates)
            has_non_banded_templates = true;

        if (prep.has_banded_templates && !prep.batched_Q.empty())
            ExecutionRunner::synthesize(
                prep.batched_Q, prep.per_q_ostats,
                this->statscomp, prep.all_slice_bounds,
                this->cmd.groups, prep.band_inner_templates);
    }

    // Non-banded synthesis: pass all Q together (SynthesisRunner already groups
    // results by oq_file, so per-workload output is produced in one pass).
    if (has_non_banded_templates || !has_banded_templates)
    {
        const vector<string>& groups = has_banded_templates
            ? non_banded_groups : this->cmd.groups;
        ExecutionRunner::synthesize(this->Q, this->Ostats, this->statscomp, groups);
    }
}


void Controller::score()
{
    Log::w(0, "Scores");
    
    Score score_display;
    score_display.process_scores(this->cmd.filter);
}


void Controller::accuracy_check()
{
    ProcessExec::python_setup(false);
    AccuracyRunner(this->statscomp, this->cmd.groups).run();
}


void Controller::noise_check()
{
    NoiseRunner(this->statscomp, this->cmd.groups).run();
}


void Controller::ranking_check()
{
    ProcessExec::python_setup(false);
    RankingRunner(this->statscomp, this->cmd.groups).run();
}


void Controller::tuning_check()
{
    TuningRunner(this->statscomp, this->cmd.groups).run();
}


void Controller::logs()
{
    string logs_cmd = Cfg::get<string>("logs") + " " + to_string(this->cmd.logs_num);
    ProcessExec::run(logs_cmd, 3, true);
}


void Controller::help()
{
    string readme_cmd = Cfg::get<string>("readme");
    ProcessExec::run(readme_cmd, 0, true);
}


void Controller::config()
{
    Log::w(0, "Configuration");

    string editor_cmd = Cfg::get<string>("editor");
    const string placeholder = "<part>";
    auto pos = editor_cmd.find(placeholder);
    if (pos != string::npos)
        editor_cmd.replace(pos, placeholder.size(), this->cmd.config_part);
    ProcessExec::run(editor_cmd, 0, true);
}


void Controller::sequence()
{
    Sequence(this->cmd.seq_flow).run();
}


void Controller::setup()
{
    Log::w(0, "Setup");

    ProcessExec::python_setup(true);
}


void Controller::clean()
{
    Log::w(0, "Cleanup");

    vector<string> artifacts;
    string clean_str = this->cmd.clean;
    stringstream ss(clean_str);
    string item;
    
    while (getline(ss, item, ',')) artifacts.push_back(item);

    Persistence::clean(artifacts);
}