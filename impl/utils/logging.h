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

#ifndef _LOGGING_H_
#define _LOGGING_H_

#include <string>
#include <vector>
#include <sstream>
#include <functional>

using namespace std;


class Log 
{
public:
    static void w(const int &level, const string &msg);
    static void w(const int &level, const string &caption, 
        const string &msg);

    template <typename T>
    static void w(const int &level,
         const string &caption, const T& value)
    {
        if constexpr (is_same_v<T, string>)
            Log::w(level, caption, value);
        else if constexpr (is_convertible_v<T, string>)
            Log::w(level, caption, string(value));
        else if constexpr (requires(const T& v) { v.str(); })
            Log::w(level, caption, value.str());
        else if constexpr (is_arithmetic_v<T>)
            Log::w(level, caption, to_string(value));
        else
        {
            ostringstream oss;
            oss << value;
            Log::w(level, caption, oss.str());
        }
    }

    template <typename T>
    static void w(const int &level, 
        const string &caption, const vector<T>& values)
    {
        if (values.empty()) return;

        Log::w(level, caption, strs(values));
    }

    template <typename T, typename Func>
    static void w(const int &level, 
        const string &caption, const vector<T>& values,
        Func selector)
    {
        if (values.empty()) return;

        Log::w(level, caption, strss(values, selector));
    }

    static int level_output;

    static int caption_width;

private:
    static string last_section;
};


string progress(
    const string &caption, 
    const int &current,
    const int &total);

string indent(int level, const string &text);

template <typename T>
string str(const vector<T> &vec, const string &delim = "; ")
{
    if (vec.empty()) return "";

    ostringstream oss;
    for (size_t i = 0; i < vec.size(); ++i)
    {
        oss << vec[i];
        if (i + 1 < vec.size()) oss << delim;
    }
    return oss.str();
}


template <typename T>
string strs(const vector<T> &vec, const string &delim = "; ")
{
    if (vec.empty()) return "";

    ostringstream oss;
    for (size_t i = 0; i < vec.size(); ++i)
    {
        if constexpr (requires(const T& v) { v.str(); })
            oss << vec[i].str();
        else
        {
            if constexpr (is_arithmetic_v<T>)
                oss << to_string(vec[i]);
            else
            {
                ostringstream tmp;
                tmp << vec[i];
                oss << tmp.str();
            }
        }
        if (i + 1 < vec.size()) oss << delim;
    }
    return oss.str();
}


template <typename T, typename Func>
string strss(const vector<T> &vec, Func selector,
    const string &delim = "; ")
{
    if (vec.empty()) return "";

    ostringstream oss;
    for (size_t i = 0; i < vec.size(); ++i)
    {
        oss << selector(vec[i]);

        if (i + 1 < vec.size()) oss << delim;
    }
    return oss.str();
}


#endif // _LOGGING_H_
