/******************************************************************************
 * Project:  synthdex
 * Purpose:  Adaptive TIR indexing
 * Author:   Christian Rauch
 ******************************************************************************
 * Copyright (c) 2025 - 2026
 *
 *
 * Extending
 *
 * Project:  irhint
 * Purpose:  Fast indexing for termporal information retrieval
 * Author:   Panagiotis Bouros, pbour@github.io
 * Author:   Christian Rauch
 ******************************************************************************
 * Copyright (c) 2023 - 2024
 *
 *
 * Extending
 *
 * Project:  hint
 * Purpose:  Indexing interval data
 * Author:   Panagiotis Bouros, pbour@github.io
 ******************************************************************************
 * Copyright (c) 2020 - 2024
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

#pragma once
#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>
#include <set>
#include <string>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <chrono>
#include <unistd.h>
#include <tuple>
#include <map>
#include <optional>
#include "logging.h"
#include "cfg.h"

using namespace std;


typedef int PartitionId;
typedef int RecordId;
typedef int Timestamp;
typedef int ElementId;


struct RangeQuery
{
	size_t id;
	Timestamp start, end;

	RangeQuery()
	{
	};
	RangeQuery(size_t i, Timestamp s, Timestamp e)
	{
		id = i;
		start = s;
		end = e;
	};
};


struct RangeIRQuery
{
	size_t id;
	Timestamp start, end;
	vector<ElementId> elems;

	RangeIRQuery()
	{
		
	};

	RangeIRQuery(size_t i, Timestamp s, Timestamp e)
	{
		id = i;
		start = s;
		end = e;
	};

	RangeIRQuery(size_t i, Timestamp s, Timestamp e, vector<ElementId>& es)
	{
		id = i;
		start = s;
		end = e;
		elems = es;
	};

	string str(bool with_id = true) const
	{
		string str = with_id ? (to_string(id) + " ") : "";
		str += to_string(start) + " " + to_string(end) + " ";

		for (auto t : elems)
			str += to_string(t) + ",";

		str.pop_back();
		return str;
	};

	static vector<ElementId> parse_elements(const string &elems_str)
	{
		vector<ElementId> elems;
		istringstream iss(elems_str);
		string elem_str;
		while (getline(iss, elem_str, ','))
			elems.push_back(stoi(elem_str));

		return elems;
	}
};



class Timer
{
private:
	using Clock = chrono::high_resolution_clock;
	Clock::time_point start_time, stop_time;
	
public:
	Timer()
	{
		start();
	}
	
	void start()
	{
		start_time = Clock::now();
	}
	
	double getElapsedTimeInSeconds()
	{
		return chrono::duration<double>(stop_time - start_time).count();
	}
	
	double stop()
	{
		stop_time = Clock::now();
		return getElapsedTimeInSeconds();
	}
};


#endif // _GLOBAL_H_
