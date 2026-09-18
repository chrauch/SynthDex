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

#ifndef _CONTROLLER_H_
#define _CONTROLLER_H_

#include "../containers/inverted_file.h"
#include "../containers/relations.h"
#include "../containers/temporal_inverted_file.h"
#include "../generation/igen.h"
#include "../generation/ogen.h"
#include "../generation/ostatsgen.h"
#include "../generation/qgen.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include "../learning/statsserializer.h"
#include "../utils/persistence.h"
#include "../utils/processexec.h"
#include "../utils/executionrunner.h"
#include "../utils/score.h"
#include "../utils/sequence.h"
#include "../learning/statsconvert.h"
#include <stdexcept>
#include <string>
#include <functional>
#include <optional>
#include <unordered_map>

using namespace std;


struct Cmd
{
	string cfg_dir     = "config";
	string stage       = "";
	bool   analyze     = false;
	bool   query       = false;
	bool   compare     = false;
	bool   update      = false;
	bool   remove      = false;
	bool   softdelete  = false;
	bool   gen_Os      = false;
	bool   gen_O       = false;
	bool   gen_Q       = false;
	bool   setup       = false;
	bool   learn       = false;
	bool   convert     = false;
	bool   predict     = false;
	bool   synth       = false;
	bool   score       = false;
	bool   accuracy    = false;
	bool   noise       = false;
	bool   ranking     = false;
	bool   tuning      = false;
	bool   logs        = false;
	bool   config      = false;
	bool   help        = false;
	string config_part = "";
	int    logs_num    = 200;
	string file_O      = "";
	string file_Q      = "";
	string file_O2     = "";
	string idxschema   = "";
	string q_pattern   = "";
	vector<string> groups;
	string filter      = "";
	string clean       = "";
	bool   seq         = false;
	string seq_flow    = "";
};


class Controller
{
public:
	Controller(Cmd &cmd);

	void start();

private:
	friend class ComparisonRunner;

	void clean();

	void analyze();

	void compare();

	void load_Q(const string& file);

	void load_O();

	void generate_Q();

	void generate_O(int &i);

	void setup();

	void learn();

	void convert();

	void synthesize();

	void score();

	void accuracy_check();

	void noise_check();

	void ranking_check();

	void tuning_check();

	void logs();

	void help();

	void config();

	int execute(ComparisonMeasurements* comparison_measurements = nullptr);

	void update();

	void remove();

	void softdelete();

	string synthesize_for_mutation();

	void sequence();

	StatsComp statscomp;
	IRelation O;
	OStats Ostats;
	string execution_O_file;
	vector<tuple<string,vector<RangeIRQuery>>> Q;
	// Maps each Q name (get<0> of the Q tuple) to the raw CLI workload pattern
	// (e.g. "1HOTA{ext.ranges=[0.1-1,1-10]}+1COLDA{...}") used to generate it.
	unordered_map<string,string> Q_patterns;
	Cmd cmd;

};

#endif // _CONTROLLER_H_