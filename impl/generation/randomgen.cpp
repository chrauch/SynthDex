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

#include "randomgen.h"
#include <random>
#include <iostream>
#include <fstream>
#include <filesystem>

using namespace std;


RandomGen::RandomGen()
    : rng(random_device{}())
{
}


int RandomGen::rndi(int lower, int upper)
{
    if (lower == upper) return lower;
    // Lower and upper are inclusive bounds
    uniform_int_distribution<> dist(lower, upper);
    return dist(this->rng);
}


double RandomGen::rndd(double lower, double upper)
{
    if (lower == upper) return lower;

    uniform_real_distribution<double> dist(lower, upper);
    return dist(this->rng);
}


double RandomGen::rndd_gauss(double avg, double dev)
{
    normal_distribution<double> dist(avg, dev);
    return dist(this->rng);
}


double RandomGen::rnddv(
    double base, double minus, double plus)
{
    return this->rndd(base - minus, base + plus);
}


double RandomGen::rnddv(
    double base, double minus_plus)
{
    return this->rndd(base - minus_plus, base + minus_plus);
}


 double RandomGen::rnd(const vector<double>& values)
{
    uniform_int_distribution<size_t> dist(0, values.size() - 1);
    return values[dist(this->rng)];
}


vector<double> RandomGen::rndds_weighted(
    size_t count, double min_val, double max_val, double total_val)
{
    vector<double> weights;
    weights.reserve(count);

    int total = 0;
    for (size_t i = 0; i < count; ++i)
    {
        double w = rndd(min_val, max_val);
        weights.push_back(w);
        total += w;
    }

    vector<double> values;
    values.reserve(count);
    for (int w : weights)
    {
        values.push_back(total_val * w / total);
    }

    return values;
}


void RandomGen::normalize(vector<double>& input, double total)
{
    double sum = accumulate(input.begin(), input.end(), 0.0);

    if (sum == 0.0)
    {
        fill(input.begin(), input.end(), 0.0);
        return;
    }

    for (double& x : input)
    {
        x = (x / sum) * total;
    }
}


vector<double> RandomGen::rnd_vary(const vector<double> &values, double off)
{
    vector<double> result;
    result.reserve(values.size());
    
    for (const auto& val : values)
    {
        double varied = val + (val * rndd(-off, off));
        result.push_back(varied);
    }
    
    return result;
}


double RandomGen::rnd_vary(double value, double off)
{
    return value + (value * rndd(-off, off));
}


double RandomGen::rnd_between(
    const double &a,
    const double &b,
    const double &off)
{
    double low  = min(a, b);
    double high = max(a, b);

    low  -= (low * off);
    high += (high * off);

    return rndd(low, high);
}



