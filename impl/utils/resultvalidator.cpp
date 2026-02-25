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

#include "resultvalidator.h"
#include <algorithm>
#include <sstream>

ResultValidator::ResultValidator(
    const IRelation& O,
    const vector<tuple<string,vector<RangeIRQuery>>>& Q,
    const OStats& Ostats,
    const bool write_O,
    const bool write_Q)
    : IndexEvaluator(O, Q, Ostats), write_O(write_O), write_Q(write_Q)
{
}

void ResultValidator::run(IRIndex* idx, const iStats& Istats, const double& _)
{
    auto reference_results = get_reference_results();
    auto test_results = get_test_results(idx);

    this->compare_and_report_results(reference_results, test_results);
}

vector<vector<RelationId>> ResultValidator::get_reference_results()
{
    vector<vector<RelationId>> reference_results;
    auto idx_reference = TemporalInvertedFile(this->O);

    for (const auto &Qx : this->Q)
    {
        reference_results.push_back({});
        for (const auto &q : get<1>(Qx))
        {
            RelationId qresult;
            idx_reference.query(q, qresult);
            sort(qresult.begin(), qresult.end());
            reference_results.back().push_back(qresult);
        }
    }

    return reference_results;
}

vector<vector<RelationId>> ResultValidator::get_test_results(
    IRIndex* test_index)
{
    vector<vector<RelationId>> test_results;

    for (const auto &Qx : this->Q)
    {
        test_results.push_back({});
        for (const auto &q : get<1>(Qx))
        {
            RelationId qresult;
            test_index->query(q, qresult);
            sort(qresult.begin(), qresult.end());
            test_results.back().push_back(qresult);
        }
    }

    return test_results;
}

void ResultValidator::compare_and_report_results(
    const vector<vector<RelationId>>& reference_results,
    const vector<vector<RelationId>>& test_results)
{
    vector<RangeIRQuery> Q_broken;

    for (size_t iQ = 0; iQ < this->Q.size(); iQ++)
    {
        auto& Q = this->Q[iQ];
        for (size_t iq = 0; iq < get<1>(Q).size(); iq++)
        {
            const auto &q = get<1>(Q)[iq];
            const auto &ref_result = reference_results[iQ][iq];
            const auto &test_result = test_results[iQ][iq];

            size_t ref_size = ref_result.size();
            size_t test_size = test_result.size();

            bool q_mismatch = ref_size != test_size;

            if (!q_mismatch)
            {
                for (size_t j = 0; j < ref_size; j++)
                {
                    if (ref_result[j] != test_result[j])
                    {
                        q_mismatch = true;
                        break;
                    }
                }
            }

            stringstream ss_ref;
            for (auto &oid : ref_result) ss_ref << oid << " ";
            
            if (q_mismatch)
            {
                Q_broken.push_back(q);

                if (Q_broken.size() <= 10)
                {
                    Log::w(1, "Wrong q " + to_string(q.id), q.str(false));
                    Log::w(2, "Results (reference)", ss_ref.str());

                    stringstream ss_test;
                    for (auto &oid : test_result) ss_test << oid << " ";
                    Log::w(2, "Results (test)", ss_test.str());
                }
                else if (Q_broken.size() == 11) Log::w(1, "...");
            }
        }
    }

    size_t total_queries = 0;
    for (const auto& Qx : this->Q) total_queries += get<1>(Qx).size();
    Log::w(1, "Num of wrong results",
        (to_string(Q_broken.size()) + " / " + to_string(total_queries)));

    if ((this->write_O || this->write_Q) && !Q_broken.empty())
    {
        if (this->write_O)
            Persistence::write_O_dat(this->O, this->Ostats);
        if (this->write_Q)
            Persistence::write_Q_dat(make_tuple("error", Q_broken), this->Ostats);
    }
}
