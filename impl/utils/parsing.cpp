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

#include "parsing.h"
#include <sstream>
#include <map>
#include <iomanip>
#include <chrono>
#include <ctime>

using namespace std;


// Trim whitespace
string Parsing::trim(const string& s)
{
    auto start = s.find_first_not_of(" \n\r\t");
    auto end = s.find_last_not_of(" \n\r\t");
    return (start == string::npos || end == string::npos) 
        ? "" : s.substr(start, end - start + 1);
}


// Expect and remove a literal
void Parsing::expect(istream& in, char expected)
{
    char c;
    do in.get(c); while (isspace(c));
    if (c != expected) 
        throw runtime_error(
            string("Expected '") + expected + "', got '" + c + "'");
}


// Parse a quoted string
string Parsing::parse_string(istream& in)
{
    Parsing::expect(in, '"');
    string result;
    char c;
    while (in.get(c)) 
    {
        if (c == '"') break;
        result += c;
    }
    return result;
}


// Skip to next non-space
void Parsing::skip_ws(istream& in)
{
    while (isspace(in.peek())) in.get();
}


// Parse a key-value pair (expecting `"key": value`)
string Parsing::parse_key(istream& in)
{
    Parsing::skip_ws(in);
    string key = Parsing::parse_string(in);
    Parsing::skip_ws(in);
    Parsing::expect(in, ':');
    Parsing::skip_ws(in);
    return key;
}
