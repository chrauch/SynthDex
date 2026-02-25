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

#ifndef _IDXSCHEMASERIALIZER_H_
#define _IDXSCHEMASERIALIZER_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "../containers/hierarchical_index.h"
#include "idxschema.h"

#include <vector>
#include <string>


class IdxSchemaSerializer
{
public:

    static IdxSchema from_json(const string &json);
    static string to_json(
        const IdxSchema& idxsch,
        int indent = 0,
        const string& tab = "\t",
        const string& nl = "\n");
    static string to_json_line(const IdxSchema& cfg);

private:
    static IdxSchema from_json_element(istream& in);
    static vector<IdxSchema> from_json_array(istream& in);
};


#endif // _IDXSCHEMASERIALIZER_H_
