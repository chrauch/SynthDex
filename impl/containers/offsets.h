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

#ifndef _OFFSETS_H_
#define _OFFSETS_H_

#include "relations.h"



class OffsetEntry_SS_CM
{
public:
    Timestamp tstamp;
    RelationIdIterator iterI;
    vector<pair<Timestamp, Timestamp> >::iterator iterT;
    PartitionId pid;
    
    OffsetEntry_SS_CM();
    OffsetEntry_SS_CM(Timestamp tstamp, RelationIdIterator iterI, vector<pair<Timestamp, Timestamp> >::iterator iterT, PartitionId pid);
    bool operator < (const OffsetEntry_SS_CM &rhs) const;
    bool operator >= (const OffsetEntry_SS_CM &rhs) const;
    ~OffsetEntry_SS_CM();
};

typedef vector<OffsetEntry_SS_CM> Offsets_SS_CM;
typedef Offsets_SS_CM::const_iterator Offsets_SS_CM_Iterator;
#endif //_OFFSETS_H_
