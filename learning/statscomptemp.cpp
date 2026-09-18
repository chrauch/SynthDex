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

#include "statscomptemp.h"
#include "../utils/global.h"
#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <iostream>

using namespace std;


StatsCompTemp::StatsCompTemp(const IRelation &O, const OStats &Ostats)
    : O(O), Ostats(Ostats)
{}


// ── helper: compute average of v ────────────────────────────────────────────
static double vec_avg(const vector<double> &v)
{
    if (v.empty()) return 0.0;
    double sum = 0.0;
    for (const auto &x : v) sum += x;
    return sum / static_cast<double>(v.size());
}


vector<OSliceStats> StatsCompTemp::compute_slices() const
{
    const int    num_slices  = Cfg::get<int>("synthesis.intervals.slices");
    const double domain      = static_cast<double>(this->Ostats.domain);
    const double slice_width = domain / num_slices;

    Log::w(1, "O-Slice analysis",
        to_string(num_slices) + " slices of width " + to_string(slice_width));

    vector<OSliceStats> result(num_slices);

    for (int s = 0; s < num_slices; ++s)
    {
        double s_start = s * slice_width;
        double s_end   = (s + 1) * slice_width;

        size_t count = 0;
        vector<double> elem_counts;

        for (const auto &rec : O)
        {
            if (rec.start < s_end && rec.end > s_start)
            {
                count++;
                elem_counts.push_back(static_cast<double>(rec.elements.size()));
            }
        }

        double avg_elems = vec_avg(elem_counts);

        Log::w(1, "  O-slice " + to_string(s),
            + " [" + to_string(static_cast<int>(s_start))
            + "-" + to_string(static_cast<int>(s_end)) + ")"
            + " o_cnt=" + to_string(count)
            + " o_avg_elems=" + to_string(avg_elems));

        result[s] = OSliceStats{};
        result[s].index      = s;
        result[s].s_start    = s_start;
        result[s].s_end      = s_end;
        result[s].count      = count;
        result[s].avg_elems  = avg_elems;
    }

    return result;
}


vector<OSliceStats> StatsCompTemp::compute_slices(vector<OSliceStats> slices, const vector<RangeIRQuery> &Q) const
{
    // Overlay Q-side stats onto the already-computed O-slice stats.
    const int    num_slices  = static_cast<int>(slices.size());
    const double dict_log    = this->Ostats.dict_log;

    vector<OSliceStats> &result = slices;

    Log::w(1, "Q-Slice analysis",
        to_string(num_slices) + " slices, " + to_string(Q.size()) + " queries");

    for (int s = 0; s < num_slices; ++s)
    {
        double s_start = result[s].s_start;
        double s_end   = result[s].s_end;

        vector<double> lengths, elem_counts, elem0_ranks;

        for (const auto &q : Q)
        {
            if (q.start >= s_end || q.end <= s_start) continue;
            lengths.push_back(static_cast<double>(q.end - q.start));
            elem_counts.push_back(static_cast<double>(q.elems.size()));
            if (!q.elems.empty() && dict_log > 0.0)
                elem0_ranks.push_back(log10(q.elems[0] + 1) / dict_log);
        }

        result[s].query_count      = lengths.size();
        result[s].avg_q_length     = vec_avg(lengths);
        result[s].avg_q_elem_count = vec_avg(elem_counts);
        result[s].avg_q_elem0_rank = vec_avg(elem0_ranks);

        Log::w(1, "  Q-slice " + to_string(s),
            + " [" + to_string(static_cast<int>(s_start))
            + "-" + to_string(static_cast<int>(s_end)) + ")"
            + " q_cnt=" + to_string(lengths.size())
            + " q_avg_len=" + to_string(result[s].avg_q_length)
            + " q_avg_elems=" + to_string(result[s].avg_q_elem_count)
            + " q_avg_rank=" + to_string(result[s].avg_q_elem0_rank));
    }

    // Interpolate Q features for zero-query slices so they don't distort clustering.
    for (int s = 0; s < num_slices; ++s)
    {
        if (result[s].query_count > 0) continue;

        // Find nearest non-empty neighbour on each side.
        int left = -1, right = -1;
        for (int l = s - 1; l >= 0; --l)
            if (result[l].query_count > 0) { left = l; break; }
        for (int r = s + 1; r < num_slices; ++r)
            if (result[r].query_count > 0) { right = r; break; }

        if (left >= 0 && right >= 0)
        {
            double t  = static_cast<double>(s - left) / (right - left);
            result[s].avg_q_length     = result[left].avg_q_length     * (1 - t) + result[right].avg_q_length     * t;
            result[s].avg_q_elem_count = result[left].avg_q_elem_count * (1 - t) + result[right].avg_q_elem_count * t;
            result[s].avg_q_elem0_rank = result[left].avg_q_elem0_rank * (1 - t) + result[right].avg_q_elem0_rank * t;
        }
        else if (left >= 0)
        {
            result[s].avg_q_length     = result[left].avg_q_length;
            result[s].avg_q_elem_count = result[left].avg_q_elem_count;
            result[s].avg_q_elem0_rank = result[left].avg_q_elem0_rank;
        }
        else if (right >= 0)
        {
            result[s].avg_q_length     = result[right].avg_q_length;
            result[s].avg_q_elem_count = result[right].avg_q_elem_count;
            result[s].avg_q_elem0_rank = result[right].avg_q_elem0_rank;
        }

        Log::w(1, "  Q-slice " + to_string(s) + " (interpolated)",
            " q_avg_len=" + to_string(result[s].avg_q_length)
            + " q_avg_elems=" + to_string(result[s].avg_q_elem_count)
            + " q_avg_rank=" + to_string(result[s].avg_q_elem0_rank));
    }

    return slices;
}


vector<SliceCluster> StatsCompTemp::cluster_slices(
    const vector<OSliceStats> &slices,
    int k_cfg,
    bool auto_k,
    int max_iter) const
{
    if (k_cfg <= 0)
        throw invalid_argument("Band count must be positive.");
    if (slices.empty())
        throw invalid_argument("Cannot cluster an empty slice vector.");

    const int n = static_cast<int>(slices.size());
    const int k = min(k_cfg, n);

    // Features: o_cnt_log, o_avg_elems_log, q_avg_len_log, avg_q_elem_count, avg_q_elem0_rank
    // o_cnt / avg_elems / q_avg_len are log10-transformed before normalisation to
    // avoid skewed ranges collapsing most slices near zero in feature space.
    // avg_q_elem_count is small integers (minor skew) and avg_q_elem0_rank is
    // already in [0,1], so both stay linear.
    double min_cnt = numeric_limits<double>::max(), max_cnt = numeric_limits<double>::lowest();
    double min_mel = numeric_limits<double>::max(), max_mel = numeric_limits<double>::lowest();
    double min_ql  = numeric_limits<double>::max(), max_ql  = numeric_limits<double>::lowest();
    double min_qe  = numeric_limits<double>::max(), max_qe  = numeric_limits<double>::lowest();
    double min_qr  = numeric_limits<double>::max(), max_qr  = numeric_limits<double>::lowest();

    for (const auto &s : slices)
    {
        double l_cnt = log10(max(1.0, static_cast<double>(s.count)));
        double l_mel = log10(max(1.0, s.avg_elems));
        double l_ql  = log10(max(1.0, s.avg_q_length));
        min_cnt = min(min_cnt, l_cnt); max_cnt = max(max_cnt, l_cnt);
        min_mel = min(min_mel, l_mel); max_mel = max(max_mel, l_mel);
        min_ql  = min(min_ql,  l_ql);  max_ql  = max(max_ql,  l_ql);
        min_qe  = min(min_qe,  s.avg_q_elem_count);
        max_qe  = max(max_qe,  s.avg_q_elem_count);
        min_qr  = min(min_qr,  s.avg_q_elem0_rank);
        max_qr  = max(max_qr,  s.avg_q_elem0_rank);
    }
    double r_cnt = max_cnt - min_cnt; if (r_cnt == 0.0) r_cnt = 1.0;
    double r_mel = max_mel - min_mel; if (r_mel == 0.0) r_mel = 1.0;
    double r_ql  = max_ql  - min_ql;  if (r_ql  == 0.0) r_ql  = 1.0;
    double r_qe  = max_qe  - min_qe;  if (r_qe  == 0.0) r_qe  = 1.0;
    double r_qr  = max_qr  - min_qr;  if (r_qr  == 0.0) r_qr  = 1.0;

    vector<vector<double>> feat(n, vector<double>(5));
    for (int i = 0; i < n; ++i)
    {
        feat[i][0] = (log10(max(1.0, static_cast<double>(slices[i].count))) - min_cnt) / r_cnt;
        feat[i][1] = (log10(max(1.0, slices[i].avg_elems))        - min_mel) / r_mel;
        feat[i][2] = (log10(max(1.0, slices[i].avg_q_length))     - min_ql)  / r_ql;
        feat[i][3] = (slices[i].avg_q_elem_count  - min_qe)  / r_qe;
        feat[i][4] = (slices[i].avg_q_elem0_rank  - min_qr)  / r_qr;
    }

    vector<int> asgn = run_clustering(feat, k, auto_k, max_iter);

    // Actual k may be less than k_cfg when auto-k selected the elbow.
    const int k_use = asgn.empty() ? 0 : *max_element(asgn.begin(), asgn.end()) + 1;

    // Build clusters
    const bool contiguous = Cfg::get<bool>("synthesis.intervals.clustering.contiguous");
    vector<SliceCluster> clusters(k_use);
    for (int j = 0; j < k_use; ++j)
    {
        clusters[j].cluster_id     = j;
        clusters[j].temporal_start = numeric_limits<double>::max();
        clusters[j].temporal_end   = numeric_limits<double>::lowest();
    }
    for (int i = 0; i < n; ++i)
    {
        int j = asgn[i];
        clusters[j].member_indices.push_back(slices[i].index);
        clusters[j].temporal_start           = min(clusters[j].temporal_start, slices[i].s_start);
        clusters[j].temporal_end             = max(clusters[j].temporal_end,   slices[i].s_end);
        clusters[j].centroid_count        += static_cast<double>(slices[i].count);
        clusters[j].centroid_avg_elems    += slices[i].avg_elems;
        clusters[j].query_count           += slices[i].query_count;
        clusters[j].centroid_q_length     += slices[i].avg_q_length;
        clusters[j].centroid_q_elem_count += slices[i].avg_q_elem_count;
        clusters[j].centroid_q_elem0_rank += slices[i].avg_q_elem0_rank;
    }
    for (int j = 0; j < k_use; ++j)
    {
        int sz = static_cast<int>(clusters[j].member_indices.size());
        if (sz > 0)
        {
            clusters[j].centroid_count        /= sz;
            clusters[j].centroid_avg_elems    /= sz;
            clusters[j].centroid_q_length     /= sz;
            clusters[j].centroid_q_elem_count /= sz;
            clusters[j].centroid_q_elem0_rank /= sz;
        }
    }

    sort(clusters.begin(), clusters.end(),
        [](const SliceCluster &a, const SliceCluster &b)
        { return a.temporal_start < b.temporal_start; });
    for (int j = 0; j < k_use; ++j) clusters[j].cluster_id = j;

    // Determine whether Q data was included
    bool has_q = false;
    for (const auto &s : slices) if (s.query_count > 0) { has_q = true; break; }

    string k_desc = to_string(k_use);
    if (auto_k && k_use < k) k_desc += " (auto, k_max=" + to_string(k) + ")";
    Log::w(1, "Slice clustering",
        k_desc + " clusters ("
        + string(contiguous ? "contiguous" : "free") + ", "
        + string(has_q ? "O+Q" : "O only") + ")");
    for (int j = 0; j < k_use; ++j)
    {
        auto &members = clusters[j].member_indices;
        sort(members.begin(), members.end());
        string ms;
        for (size_t m = 0; m < members.size(); ++m) { if (m) ms += ","; ms += to_string(members[m]); }
        string msg =
            "slices=[" + ms + "]"
            + " temporal=[" + to_string(static_cast<int>(clusters[j].temporal_start))
            + "," + to_string(static_cast<int>(clusters[j].temporal_end)) + ")"
            + " o_cnt=" + to_string(clusters[j].centroid_count)
            + " o_avg_elems=" + to_string(clusters[j].centroid_avg_elems);
        if (has_q)
            msg += " q_cnt=" + to_string(clusters[j].query_count)
                 + " q_avg_len=" + to_string(clusters[j].centroid_q_length)
                 + " q_avg_elems=" + to_string(clusters[j].centroid_q_elem_count)
                 + " q_avg_rank=" + to_string(clusters[j].centroid_q_elem0_rank);
        Log::w(1, "  cluster " + to_string(j), msg);
    }
    return clusters;
}




// ── kneedle elbow: returns 1-based k at the knee ────────────────────────────
int StatsCompTemp::select_k_elbow(const vector<double> &sse)
{
    const int m = static_cast<int>(sse.size()); // m == k_max
    if (m <= 1) return 1;

    double sse_max = sse[0];          // SSE for k=1  (largest)
    double sse_min = sse[m - 1];      // SSE for k=k_max (smallest)

    // Flat curve, no useful elbow, use minimum k
    if (sse_max - sse_min < 1e-12) return 1;

    // Normalize: x in [0,1] over k=1..k_max, y in [0,1] over SSE.
    // The reference line runs from (0,1) to (1,0).  For a monotonically
    // decreasing SSE curve every interior point lies BELOW this line
    // (x+y < 1).  The elbow is the point farthest BELOW it, i.e. we
    // maximise d = 1 - x - y.
    int    best_k    = 1;
    double best_dist = -1.0;
    for (int i = 0; i < m; ++i)
    {
        double x = (m > 1) ? static_cast<double>(i) / (m - 1) : 0.0;
        double y = (sse[i] - sse_min) / (sse_max - sse_min);
        double d = 1.0 - x - y;  // proportional to perpendicular distance below the diagonal
        if (d > best_dist) { best_dist = d; best_k = i + 1; }
    }
    return best_k;
}

vector<int> StatsCompTemp::run_clustering(
    const vector<vector<double>> &feat,
    int k,
    bool auto_k,
    int max_iter) const
{
    const bool contiguous = Cfg::get<bool>("synthesis.intervals.clustering.contiguous");
    const int  n          = static_cast<int>(feat.size());
    const int  dim        = feat.empty() ? 0 : static_cast<int>(feat[0].size());
    const int  k_max      = k;

    // SSE of feat[l..r] (inclusive)
    auto seg_cost = [&](int l, int r) -> double
    {
        vector<double> mean(dim, 0.0);
        int len = r - l + 1;
        for (int i = l; i <= r; ++i)
            for (int d = 0; d < dim; ++d) mean[d] += feat[i][d];
        for (auto &v : mean) v /= len;
        double cost = 0.0;
        for (int i = l; i <= r; ++i)
            for (int d = 0; d < dim; ++d) { double x = feat[i][d] - mean[d]; cost += x * x; }
        return cost;
    };

    vector<int> assignments(n, 0);

    if (contiguous)
    {
        const double INF = numeric_limits<double>::max() / 2.0;
        // Always run DP to k_max; auto-k reads the curve for free.
        vector<vector<double>> dp(k_max + 1, vector<double>(n, INF));
        vector<vector<int>>    sp(k_max + 1, vector<int>(n, 0));

        for (int j = 0; j < n; ++j)
            dp[1][j] = seg_cost(0, j);

        for (int seg = 2; seg <= k_max; ++seg)
            for (int j = seg - 1; j < n; ++j)
                for (int l = seg - 1; l <= j; ++l)
                {
                    double cost = dp[seg - 1][l - 1] + seg_cost(l, j);
                    if (cost < dp[seg][j]) { dp[seg][j] = cost; sp[seg][j] = l; }
                }

        // Select k
        int k_use = k_max;
        if (auto_k && k_max > 1)
        {
            vector<double> sse(k_max);
            for (int seg = 1; seg <= k_max; ++seg)
                sse[seg - 1] = dp[seg][n - 1];
            k_use = select_k_elbow(sse);
            Log::w(1, "  auto-k", "selected k=" + to_string(k_use)
                + " (k_max=" + to_string(k_max) + ")");
        }

        // Backtrack for k_use
        vector<int> boundaries;
        int right = n - 1;
        for (int seg = k_use; seg >= 2; --seg)
        {
            boundaries.push_back(sp[seg][right]);
            right = sp[seg][right] - 1;
        }
        boundaries.push_back(0);
        reverse(boundaries.begin(), boundaries.end());

        for (int seg = 0; seg < k_use; ++seg)
        {
            int lo = boundaries[seg];
            int hi = (seg + 1 < k_use) ? boundaries[seg + 1] - 1 : n - 1;
            for (int i = lo; i <= hi; ++i) assignments[i] = seg;
        }
    }
    else
    {
        // Helper: run k-means for a fixed k, return (assignments, SSE)
        auto run_kmeans = [&](int ki) -> pair<vector<int>, double>
        {
            vector<vector<double>> centroids(ki, vector<double>(dim, 0.0));
            for (int i = 0; i < ki; ++i)
            {
                int idx = static_cast<int>(round(
                    i * static_cast<double>(n - 1) / (ki - 1 > 0 ? ki - 1 : 1)));
                centroids[i] = feat[min(idx, n - 1)];
            }
            vector<int> asgn(n, 0);
            for (int iter = 0; iter < max_iter; ++iter)
            {
                bool changed = false;
                for (int i = 0; i < n; ++i)
                {
                    double best = numeric_limits<double>::max(); int bk = 0;
                    for (int j = 0; j < ki; ++j)
                    {
                        double d = 0.0;
                        for (int dj = 0; dj < dim; ++dj)
                        { double x = feat[i][dj] - centroids[j][dj]; d += x * x; }
                        if (d < best) { best = d; bk = j; }
                    }
                    if (asgn[i] != bk) { asgn[i] = bk; changed = true; }
                }
                if (!changed) break;
                centroids.assign(ki, vector<double>(dim, 0.0));
                vector<int> cnts(ki, 0);
                for (int i = 0; i < n; ++i)
                {
                    for (int dj = 0; dj < dim; ++dj) centroids[asgn[i]][dj] += feat[i][dj];
                    cnts[asgn[i]]++;
                }
                for (int j = 0; j < ki; ++j)
                    if (cnts[j] > 0)
                        for (int dj = 0; dj < dim; ++dj) centroids[j][dj] /= cnts[j];
            }
            // Compute total within-cluster SSE
            double sse = 0.0;
            for (int i = 0; i < n; ++i)
                for (int dj = 0; dj < dim; ++dj)
                { double x = feat[i][dj] - centroids[asgn[i]][dj]; sse += x * x; }
            return { asgn, sse };
        };

        if (auto_k && k_max > 1)
        {
            vector<double>      sse_vec(k_max);
            vector<vector<int>> asgn_by_k(k_max);
            for (int ki = 1; ki <= k_max; ++ki)
            {
                auto [a, s]    = run_kmeans(ki);
                asgn_by_k[ki - 1] = move(a);
                sse_vec[ki - 1]   = s;
            }
            int k_use = select_k_elbow(sse_vec);
            Log::w(1, "  auto-k", "selected k=" + to_string(k_use)
                + " (k_max=" + to_string(k_max) + ")");
            return asgn_by_k[k_use - 1];
        }
        else
        {
            auto [a, _]  = run_kmeans(k_max);
            assignments  = move(a);
        }
    }

    return assignments;
}
