#pragma once
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

#ifndef _CFG_H_
#define _CFG_H_

#include <sstream>
#include <map>
#include <omp.h>
#include <string>
#include <map>
#include <vector>
#include <fstream>
#include <stdexcept>
#include <type_traits>
#include "logging.h"

using namespace std;


struct SetValue
{
    enum Type
    {
        Null,
        Number,
        String,
        Object,
        Boolean,
        Array
    };
    Type type;
    double number_value;
    string string_value;
    bool bool_value;
    map<string, SetValue> object_values;
    vector<SetValue> array_values;

    SetValue();
    SetValue(double val);
    SetValue(const string &val);
    SetValue(bool val);
};


class Cfg
{

public:
    Cfg(const string &config_dir);

    string config_dir;
    static Cfg &instance();
    static Cfg *singleton;

    static int get_threads()
    {
        auto num_threads = Cfg::get<int>("threads");
        if(num_threads <= 0) num_threads = omp_get_max_threads();

        return num_threads;
    }

    template <typename T>
    static T get(const string &path)
    {
        return instance().get_as<T>(path);
    }

    static string get_json(const string &path)
    {
        return instance().get_json_at(path);
    }

    template <typename T>
    T get_as(const string &path) const
    {
        const SetValue &val = get(path);

        if constexpr (is_same_v<T, int>)
        {
            if (val.type != SetValue::Number)
                throw runtime_error("Type mismatch");
            return static_cast<int>(val.number_value);
        }
        else if constexpr (is_same_v<T, double>)
        {
            if (val.type != SetValue::Number)
                throw runtime_error("Type mismatch");
            return val.number_value;
        }
        else if constexpr (is_same_v<T, string>)
        {
            if (val.type != SetValue::String)
                throw runtime_error("Type mismatch");
            return val.string_value;
        }
        else if constexpr (is_same_v<T, bool>)
        {
            if (val.type != SetValue::Boolean)
                throw runtime_error("Type mismatch");
            return val.bool_value;
        }
        else if constexpr (is_same_v<T, vector<int>>)
        {
            if (val.type != SetValue::Array)
                throw runtime_error("Type mismatch");
            vector<int> result;
            for (const auto& v : val.array_values)
            {
                if (v.type != SetValue::Number)
                    throw runtime_error("Array element type mismatch");
                result.push_back(static_cast<int>(v.number_value));
            }
            return result;
        }
        else if constexpr (is_same_v<T, vector<double>>)
        {
            if (val.type != SetValue::Array)
                throw runtime_error("Type mismatch");
            vector<double> result;
            for (const auto& v : val.array_values)
            {
                if (v.type != SetValue::Number)
                    throw runtime_error("Array element type mismatch");
                result.push_back(v.number_value);
            }
            return result;
        }
        else if constexpr (is_same_v<T, vector<string>>)
        {
            if (val.type != SetValue::Array)
                throw runtime_error("Type mismatch");
            vector<string> result;
            for (const auto& v : val.array_values)
            {
                if (v.type != SetValue::String)
                    throw runtime_error("Array element type mismatch");
                result.push_back(v.string_value);
            }
            return result;
        }
        else if constexpr (is_same_v<T, vector<bool>>)
        {
            if (val.type != SetValue::Array)
                throw runtime_error("Type mismatch");
            vector<bool> result;
            for (const auto& v : val.array_values)
            {
                if (v.type != SetValue::Boolean)
                    throw runtime_error("Array element type mismatch");
                result.push_back(v.bool_value);
            }
            return result;
        }
        throw runtime_error("Unsupported type");
    }

    string str() const;
    string str(const SetValue &value, int indent) const;
    string get_json_at(const string &path) const;

private:

    SetValue root;

    SetValue parse(istream &in);
    const SetValue &get(const string &path) const;
};

#endif // _CFG_H_
