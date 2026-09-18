/******************************************************************************
 * Project:  SynthDex
 * Purpose:  Adaptive Ensemble Indexing for Temporal Information Retrieval via Learned Cost Models
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

#ifndef _STATSCOMPTEMP_H_
#define _STATSCOMPTEMP_H_


#include "../utils/global.h"
#include "../structure/framework.h"
#include "stats.h"
#include <cstdlib>
#include <cmath>
#include <limits>
#include <vector>
#include <string>
using namespace std;


struct OSliceStats
{
    int    index        = 0;
    double s_start      = 0.0;
    double s_end        = 0.0;
    // O-side stats
    size_t count               = 0;
    double avg_elems           = 0.0;
    // Q-side stats (populated only when a query workload is provided)
    size_t query_count         = 0;
    double avg_q_length        = 0.0;  // avg (q.end - q.start) of queries hitting slice
    double avg_q_elem_count    = 0.0;  // avg |q.elems|
    double avg_q_elem0_rank    = 0.0;  // avg log10(q.elems[0]+1)/log10(dict) in [0,1]
};

struct SliceCluster
{
    int    cluster_id             = 0;
    // O-based centroids
    double centroid_count            = 0.0;
    double centroid_avg_elems        = 0.0;
    // Q-based aggregates (populated when Q data is present at clustering time)
    size_t query_count            = 0;     // total queries across member slices
    double centroid_q_length      = 0.0;
    double centroid_q_elem_count  = 0.0;
    double centroid_q_elem0_rank  = 0.0;
    double temporal_start         = 0.0;  // min s_start of member slices
    double temporal_end           = 0.0;  // max s_end   of member slices
    vector<int> member_indices;
};


class StatsCompTemp
{
public:
    // O and Ostats are borrowed for the lifetime of this object.
    StatsCompTemp(const IRelation &O, const OStats &Ostats);

    // Divide the relation into uniform temporal slices (O stats only).
    vector<OSliceStats> compute_slices() const;

    // Fills Q-side stats into an already-computed set of O-slices.
    // Call compute_slices() first, then pass the result here.
    vector<OSliceStats> compute_slices(vector<OSliceStats> slices, const vector<RangeIRQuery> &Q) const;

    // Cluster slices using all available features (O + Q if query_count > 0).
    // When no Q data is present the Q features are all zero and do not
    // influence the clustering (normalization degrades them to constants).
    // k: number of clusters (or max if auto_k is true).
    // auto_k: if true, use kneedle elbow to select k in [1..k].
    vector<SliceCluster> cluster_slices(
        const vector<OSliceStats>  &slices,
        int k,
        bool auto_k = false,
        int max_iter = 100) const;

private:
    const IRelation &O;
    const OStats    &Ostats;

    // Shared clustering engine.  feat[i] is the pre-normalised feature vector
    // of slice i.  Reads clustering.contiguous from Cfg.
    // Returns one cluster-id per slice.
    vector<int> run_clustering(
        const vector<vector<double>> &feat,
        int k,
        bool auto_k,
        int max_iter) const;

    // Kneedle elbow: given SSE[0]=SSE for k=1, SSE[1]=SSE for k=2, …,
    // returns the 1-based k at the knee of the curve.
    static int select_k_elbow(const vector<double> &sse_by_k);
};

#endif // _STATSCOMPTEMP_H_
