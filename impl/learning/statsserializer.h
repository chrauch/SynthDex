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
#ifndef _OSTATSSERIALIZER_H_
#define _OSTATSSERIALIZER_H_

#include "../utils/global.h"
#include "stats.h"

#include <vector>
#include <string>

class StatsSerializer
{
public:
    static const string sep;

    static OStats from_json(const string &str);
    static string to_json(const OStats &os, const bool &all);

    static string to_csv_header(const OStats &os, const bool &all);
    static string to_csv(const OStats &os, const bool &all);

    static string to_csv_header(const iStats &is, const bool &all);
    static string to_csv(const iStats &is, const bool &all);

    static string to_csv_header(const qStats &is, const bool &all);
    static string to_csv(const qStats &is, const bool &all);

    static string to_csv_header(const pStats &is, const bool &all);
    static string to_csv(const pStats &is, const bool &all);

    static string map_enc(int index);
};


#endif // _OSTATSSERIALIZER_H_
