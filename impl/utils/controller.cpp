/******************************************************************************
 * Project:  synthdex
 * Purpose:  Adaptive TIR indexing
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
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>
#include <sstream>

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
    if (this->cmd.logs)
    {
        this->logs();
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
    
            if (this->cmd.gen_Q)
            {
                this->generate_Q();

                if (this->cmd.query || this->cmd.predict)
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


int Controller::execute()
{
    ExecutionRunner runner(this->cmd, this->O, this->Q,
        this->Ostats, this->statscomp);
    return runner.execute();
}


void Controller::load_O()
{
    Log::w(0, "Objects");
    this->O = Persistence::read_O_dat(this->cmd.file_O);

    this->Ostats = this->statscomp.analyze_O(this->O, this->cmd.file_O);

    Persistence::write_O_stats(this->Ostats, 0);
}


void Controller::load_Q(const string& file)
{
    Log::w(0, "Queries");

    stringstream ss(file);
    string qfile;
    while (getline(ss, qfile, '|'))
    {
        if (qfile.empty()) continue;
        auto Qx = Persistence::read_q_dat(qfile);
        for (auto& q : Qx)
            this->Q.push_back(move(q));
    }
}


void Controller::update()
{
    ExecutionRunner::update(this->cmd, this->statscomp);
}


void Controller::remove()
{
    ExecutionRunner::remove(this->cmd, this->statscomp);
}


void Controller::softdelete()
{
    ExecutionRunner::softdelete(this->cmd, this->statscomp);
}


void Controller::analyze()
{
    auto O = Persistence::read_O_dat(this->cmd.file_O);
    this->Ostats = this->statscomp.analyze_O(O, this->cmd.file_O);

    Persistence::write_O_stats(this->Ostats, 0);

    if (!this->cmd.file_Q.empty())
    {
        auto Qx = Persistence::read_q_dat(this->cmd.file_Q);
        for (const auto &[pattern, Q] : Qx) this->statscomp.analyze_Q(Q);
        Persistence::write_Q_stats_csv(this->statscomp.Qstats);
    }
}


void Controller::generate_Q()
{
    Log::w(0, "Queries");

    this->Q = QGen(this->O, this->Ostats).construct_Q();

    if (!this->cmd.query && !this->cmd.predict && !this->cmd.synth)
        for (auto& Q : this->Q)
            Persistence::write_Q_dat(Q, this->Ostats);
}


void Controller::generate_O(int &i)
{
    Log::w(0, progress("Objects", i, Cfg::get<int>("o.num")));

    this->Ostats = OStatsGen().create(i);

    if (!this->cmd.gen_O || Cfg::get<bool>("out.detailed"))
        Persistence::write_O_stats(this->Ostats, i);

    if (!this->cmd.gen_O) return;

    this->O = OGen(this->Ostats).construct_O();

    if (!this->cmd.query) Persistence::write_O_dat(this->O, this->Ostats);

    this->Ostats = this->statscomp.analyze_O(
        this->O, this->Ostats.name + "-generated");

    Persistence::write_O_stats(this->Ostats, i);
}


void Controller::learn()
{
    ProcessExec::python_setup(false);

    Log::w(0, "Learning");

    ProcessExec::python_run("Training",
        "./learning/trainer.py " + this->cmd.cfg_dir, 1);
}





void Controller::synthesize()
{
    this->load_O();

    if (this->cmd.gen_Q)
        this->generate_Q();
    else
        this->load_Q(this->cmd.file_Q);

    ExecutionRunner::synthesize(this->cmd, this->Q, this->Ostats, this->statscomp);
}


void Controller::score()
{
    Log::w(0, "Scores");
    
    Score score_display;
    score_display.process_scores(this->cmd.filter);
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
    ProcessExec::run(editor_cmd, 0, true);
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