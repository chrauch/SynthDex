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

#ifndef _RANDOMGEN_H_
#define _RANDOMGEN_H_


#include "../utils/global.h"
#include "../learning/stats.h"
#include "../learning/statscomp.h"
#include <random>

using namespace std;


class RandomGen 
{
public:
    RandomGen();

    void normalize(vector<double>& input, double total);

    int rndi(int lower, int upper);

    double rndd(double lower, double upper);

    double rndd_gauss(double avg, double dev);

    double rnddv(double base, double minus, double plus);

    double rnddv(double base, double minus_plus);

    vector<double> rndds_weighted(
        size_t count, double min_val, double max_val, double total_val);
    
    double rnd(const vector<double>& values);

    template <typename T>
    vector<T> rnd_select(const vector<T> &values, const size_t select)
    {
        if (values.empty())
            throw invalid_argument("Cannot select from empty vector.");

        vector<T> shuffled = values;
        shuffle(shuffled.begin(), shuffled.end(), this->rng);

        if (select <= values.size())
        {
            shuffled.resize(select);
        }
        else
        {
            uniform_int_distribution<size_t> dist(0, values.size() - 1);
            for (size_t i = values.size(); i < select; i++)
            {
                shuffled.push_back(values[dist(this->rng)]);
            }
        }

        return shuffled;
    }

    double rnd_vary(double value, double off);
    vector<double> rnd_vary(const vector<double> &values, double off);

    double rnd_between(
        const double &a, const double &b, const double &off);

    mt19937 rng; // Persistent random number generator
};

#endif // _RANDOMGEN_H_