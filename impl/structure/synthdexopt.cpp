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

#include "synthdexopt.h"
#include "../utils/parsing.h"
#include "../terminal/tif.h"
#include "../terminal/irhint.h"
#include "../terminal/tifhintg.h"
#include "../terminal/tifslicing.h"
#include "../terminal/tifsharding.h"
#include <omp.h>


// Factory function specializations for terminal creation
template<>
unique_ptr<tIF> create_terminal<tIF>(const NodeArgs& args) {
    return make_unique<tIF>(args.boundaries, args.O);
}

template<>
unique_ptr<tIFSLICING> create_terminal<tIFSLICING>(const NodeArgs& args) {
    // Parse: tif;slicing;dyn;[1.8-2.6];[0.3-0.9] or tif;slicing;static;partitions
    if (args.method.size() < 4) 
        throw runtime_error("Invalid tIFSLICING schema");
    
    if (args.method[2] == "dyn") {
        double dynbase = stod(args.method[3]);
        double dynratio = stod(args.method[4]);
        return make_unique<tIFSLICING>(args.boundaries, 0, dynbase, dynratio, args.O);
    } else if (args.method[2] == "static") {
        int partitions = stoi(args.method[3]);
        return make_unique<tIFSLICING>(args.boundaries, partitions, 0.0, 0.0, args.O);
    }
    throw runtime_error("Invalid tIFSLICING mode");
}

template<>
unique_ptr<tIFSHARDING> create_terminal<tIFSHARDING>(const NodeArgs& args) {
    if (args.method.size() < 4)
        throw runtime_error("Invalid tIFSHARDING schema");
    
    if (args.method[2] == "static") {
        float relaxation = stof(args.method[3]);
        return make_unique<tIFSHARDING>(args.boundaries, relaxation, 0.0, 0.0, args.O);
    } else if (args.method[2] == "dyn") {
        double dynbase = stod(args.method[3]);
        double dynratio = stod(args.method[4]);
        return make_unique<tIFSHARDING>(args.boundaries, 0.0, dynbase, dynratio, args.O);
    }
    throw runtime_error("Invalid tIFSHARDING mode");
}

template<>
unique_ptr<tIFHINTg> create_terminal<tIFHINTg>(const NodeArgs& args) {
    // Parse: tif;hint;mrg;dyn;[1.3-4.0];[0.5-4.0] or tif;hint;mrg;static;bits
    if (args.method.size() < 5)
        throw runtime_error("Invalid tIFHINTg schema");
    
    if (args.method[3] == "dyn") {
        double dynbase = stod(args.method[4]);
        double dynratio = stod(args.method[5]);
        return make_unique<tIFHINTg>(args.boundaries, 0, dynbase, dynratio, args.O);
    } else if (args.method[3] == "static") {
        int bits = stoi(args.method[4]);
        return make_unique<tIFHINTg>(args.boundaries, bits, 0.0, 0.0, args.O);
    }
    throw runtime_error("Invalid tIFHINTg mode");
}

template<>
unique_ptr<irHINTa> create_terminal<irHINTa>(const NodeArgs& args) {
    // Parse: irhint;perf;[5-16]
    if (args.method.size() < 3)
        throw runtime_error("Invalid irHINTa schema");
    
    int bits = stoi(args.method[2]);
    return make_unique<irHINTa>(args.boundaries, bits, args.O);
}


SynthDexOpt::SynthDexOpt(
    const IRelation &O,
    const IdxSchema &schema,
    const OStats &ostats)
    : schema(schema),
      ostats(ostats),
      index(this->construct(O, schema, Boundaries()))
{
}


Moveout* SynthDexOpt::construct(
    const IRelation &O,
    const IdxSchema &schema,
    const Boundaries &boundaries)
{
    // First try pattern matching for known configurations
    auto matched = this->try_pattern_match(O, schema, boundaries);
    if (matched != nullptr)
        return matched;
    
    // Fall back to runtime construction
    return this->new_node(O, schema, boundaries);
}


string SynthDexOpt::method_signature(const vector<string> &method)
{
    if (method.empty()) return "";
    
    ostringstream oss;
    if (method[0] == "tif" && method.size() > 1)
    {
        oss << "tif+" << method[1];
    }
    else if (method[0] == "irhint")
    {
        oss << "irhint";
    }
    else
    {
        for (size_t i = 0; i < method.size(); ++i)
        {
            if (i > 0) oss << "+";
            oss << method[i];
        }
    }
    return oss.str();
}


Moveout* SynthDexOpt::try_pattern_match(
    const IRelation &O,
    const IdxSchema &schema,
    const Boundaries &boundaries)
{
    // Pattern match for refinement fanouts
    if (!schema.refine.empty() && schema.refine == "elemfreq")
    {
        if (schema.fanout.size() == 2)
        {
            auto sig0 = method_signature(schema.fanout[0].method);
            auto sig1 = method_signature(schema.fanout[1].method);
            
            // refine-elemfreq_tif+slicing_irhint: RefinementFanout_ElemFreq_T<tIFSLICING, irHINTa>
            if (sig0 == "tif+slicing" && sig1 == "irhint")
            {
                Timestamp start = 0, end = O.gend;
                
                // Prepare args for each child
                ElementId elem_min0 = 0, elem_max0 = this->ostats.dict;
                parse_min_max(schema.fanout[0].range, elem_min0, elem_max0, 
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b0(elem_min0, elem_max0, 0, numeric_limits<Timestamp>::max());
                NodeArgs args0(O, b0, schema.fanout[0].method, this->ostats);
                
                ElementId elem_min1 = 0, elem_max1 = this->ostats.dict;
                parse_min_max(schema.fanout[1].range, elem_min1, elem_max1,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b1(elem_min1, elem_max1, 0, numeric_limits<Timestamp>::max());
                NodeArgs args1(O, b1, schema.fanout[1].method, this->ostats);
                
                // Create template-based refinement fanout
                return new RefinementFanout_ElemFreq_T<tIFSLICING, irHINTa>(
                    start, end,
                    make_pair(b0, create_terminal<tIFSLICING>(args0)),
                    make_pair(b1, create_terminal<irHINTa>(args1))
                );
            }
            // refine-elemfreq_tif+hint_irhint: RefinementFanout_ElemFreq_T<tIFHINTg, irHINTa>
            else if (sig0 == "tif+hint" && sig1 == "irhint")
            {
                Timestamp start = 0, end = O.gend;
                
                ElementId elem_min0 = 0, elem_max0 = this->ostats.dict;
                parse_min_max(schema.fanout[0].range, elem_min0, elem_max0,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b0(elem_min0, elem_max0, 0, numeric_limits<Timestamp>::max());
                NodeArgs args0(O, b0, schema.fanout[0].method, this->ostats);
                
                ElementId elem_min1 = 0, elem_max1 = this->ostats.dict;
                parse_min_max(schema.fanout[1].range, elem_min1, elem_max1,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b1(elem_min1, elem_max1, 0, numeric_limits<Timestamp>::max());
                NodeArgs args1(O, b1, schema.fanout[1].method, this->ostats);
                
                return new RefinementFanout_ElemFreq_T<tIFHINTg, irHINTa>(
                    start, end,
                    make_pair(b0, create_terminal<tIFHINTg>(args0)),
                    make_pair(b1, create_terminal<irHINTa>(args1))
                );
            }
        }
        else if (schema.fanout.size() == 3)
        {
            auto sig0 = method_signature(schema.fanout[0].method);
            auto sig1 = method_signature(schema.fanout[1].method);
            auto sig2 = method_signature(schema.fanout[2].method);
            
            // refine-elemfreq_tif+basic_tif+slicing_irhint: RefinementFanout_ElemFreq_T<tIF, tIFSLICING, irHINTa>
            if (sig0 == "tif+basic" && sig1 == "tif+slicing" && sig2 == "irhint")
            {
                Timestamp start = 0, end = O.gend;
                
                ElementId elem_min0 = 0, elem_max0 = this->ostats.dict;
                parse_min_max(schema.fanout[0].range, elem_min0, elem_max0,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b0(elem_min0, elem_max0, 0, numeric_limits<Timestamp>::max());
                NodeArgs args0(O, b0, schema.fanout[0].method, this->ostats);
                
                ElementId elem_min1 = 0, elem_max1 = this->ostats.dict;
                parse_min_max(schema.fanout[1].range, elem_min1, elem_max1,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b1(elem_min1, elem_max1, 0, numeric_limits<Timestamp>::max());
                NodeArgs args1(O, b1, schema.fanout[1].method, this->ostats);
                
                ElementId elem_min2 = 0, elem_max2 = this->ostats.dict;
                parse_min_max(schema.fanout[2].range, elem_min2, elem_max2,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b2(elem_min2, elem_max2, 0, numeric_limits<Timestamp>::max());
                NodeArgs args2(O, b2, schema.fanout[2].method, this->ostats);
                
                return new RefinementFanout_ElemFreq_T<tIF, tIFSLICING, irHINTa>(
                    start, end,
                    make_pair(b0, create_terminal<tIF>(args0)),
                    make_pair(b1, create_terminal<tIFSLICING>(args1)),
                    make_pair(b2, create_terminal<irHINTa>(args2))
                );
            }
            // refine-elemfreq_tif+basic_tif+hint_irhint: RefinementFanout_ElemFreq_T<tIF, tIF HINTg, irHINTa>
            else if (sig0 == "tif+basic" && sig1 == "tif+hint" && sig2 == "irhint")
            {
                Timestamp start = 0, end = O.gend;
                
                ElementId elem_min0 = 0, elem_max0 = this->ostats.dict;
                parse_min_max(schema.fanout[0].range, elem_min0, elem_max0,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b0(elem_min0, elem_max0, 0, numeric_limits<Timestamp>::max());
                NodeArgs args0(O, b0, schema.fanout[0].method, this->ostats);
                
                ElementId elem_min1 = 0, elem_max1 = this->ostats.dict;
                parse_min_max(schema.fanout[1].range, elem_min1, elem_max1,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b1(elem_min1, elem_max1, 0, numeric_limits<Timestamp>::max());
                NodeArgs args1(O, b1, schema.fanout[1].method, this->ostats);
                
                ElementId elem_min2 = 0, elem_max2 = this->ostats.dict;
                parse_min_max(schema.fanout[2].range, elem_min2, elem_max2,
                              this->ostats.elements.back().hi_rank_log, true);
                Boundaries b2(elem_min2, elem_max2, 0, numeric_limits<Timestamp>::max());
                NodeArgs args2(O, b2, schema.fanout[2].method, this->ostats);
                
                return new RefinementFanout_ElemFreq_T<tIF, tIFHINTg, irHINTa>(
                    start, end,
                    make_pair(b0, create_terminal<tIF>(args0)),
                    make_pair(b1, create_terminal<tIFHINTg>(args1)),
                    make_pair(b2, create_terminal<irHINTa>(args2))
                );
            }
        }
    }
    
    // Pattern match for split fanouts
    if (!schema.split.empty() && schema.split == "temporal")
    {
        if (schema.fanout.size() == 2)
        {
            auto sig0 = method_signature(schema.fanout[0].method);
            auto sig1 = method_signature(schema.fanout[1].method);
            
            // split-temporal_tif+basic_irhint: SplitFanout_Temporal_T<tIF, irHINTa>
            if (sig0 == "tif+basic" && sig1 == "irhint")
            {
                // For split/slice, need to fragment O by time and construct with local coords
                // This is complex, so for now we'll indicate pattern and fall back
                // TODO: Full implementation with O fragmentation
            }
        }
    }
    
    // Pattern match for slice fanouts
    if (!schema.slice.empty() && schema.slice == "temporal")
    {
        if (schema.fanout.size() == 2)
        {
            auto sig0 = method_signature(schema.fanout[0].method);
            auto sig1 = method_signature(schema.fanout[1].method);
            
            // slice-temporal_tif+basic_irhint: SliceFanout_Temporal_T<tIF, irHINTa>
            if (sig0 == "tif+basic" && sig1 == "irhint")
            {
                // TODO: Implement with O slicing
            }
        }
    }
    
    // No pattern match found
    return nullptr;
}


Moveout* SynthDexOpt::new_node(
    const IRelation &O,
    const IdxSchema &schema,
    const Boundaries &boundaries)
{
    if (!schema.fanout.empty())
        return this->new_fanout_node(O, schema, boundaries);
    else if (!schema.method.empty())
        return this->new_terminal_node(O, schema.method, boundaries);
    else
        throw runtime_error("Expected terminal or fanout node");
}


void SynthDexOpt::parse_min_max(
    const string &range, int &min_v, int &max_v, const double &max_log, const bool &log_scale)
{
    size_t pos = range.find('-');
    if (pos != string::npos)
    {
        double min_rel = stod(range.substr(0, pos));
        double max_rel = stod(range.substr(pos + 1));

        if (log_scale)
        {
            int min_calc = min_rel == 0 ? 0 : (int)ceil(pow(10, min_rel * max_log));
            int max_calc = (int)ceil(pow(10, max_rel * max_log));

            min_v = max(min_v, min_calc);
            max_v = min(max_v, max_calc);
        }
        else
        {
            int max = (int)pow(10, max_log);
            min_v = (int)ceil(min_rel * max);
            max_v = (int)ceil(max_rel * max);
        }
    }
    else
        throw runtime_error("Wrong min-max format");
}


Moveout* SynthDexOpt::new_fanout_node(
    const IRelation &O,
    const IdxSchema &schema,
    const Boundaries &boundaries)
{
    if (!schema.hybrid.empty())
        return this->new_fanout_node_hybrid(schema, O);
    else if (!schema.refine.empty())
        return this->new_fanout_node_refined(schema, O);
    else if (!schema.split.empty())
        return this->new_fanout_node_splitted(schema, O);
    else if (!schema.slice.empty())
        return this->new_fanout_node_sliced(schema, O);
    else
        throw runtime_error("Expected hybrid, split, slice, or refine for a fanout node");
}


Moveout* SynthDexOpt::new_terminal_node(
    const IRelation &O,
    const vector<string> &method,
    const Boundaries &boundaries)
{
    if (method[0] == "tif")
    {
        if (method.size() > 1)
        {
            if (method[1] == "basic")
                return new tIF(boundaries, O);
            else if (method[1] == "slicing")
            {
                if (method.size() != 4 && method.size() != 5)
                    throw runtime_error("Invalid index schema");

                if (method[2] == "dyn")
                {
                    double dynbase = stod(method[3]);
                    double dynratio = stod(method[4]);
                    return new tIFSLICING(boundaries, 0, dynbase, dynratio, O);
                }
                else if (method[2] == "static")
                {
                    int partitions = stoi(method[3]);
                    return new tIFSLICING(boundaries, partitions, 0.0, 0.0, O);
                }
                else
                    throw runtime_error("Invalid index schema");
            }
            else if (method[1] == "sharding")
            {
                if (method[2] == "static")
                {
                    float relaxation = stof(method[3]);
                    return new tIFSHARDING(boundaries, relaxation, 0.0, 0.0, O);
                }
                else if (method[2] == "dyn")
                {
                    double dynbase = stod(method[3]);
                    double dynratio = stod(method[4]);
                    return new tIFSHARDING(boundaries, 0.0, dynbase, dynratio, O);
                }
                else
                    throw runtime_error("Invalid index schema");
            }
            else if (method[1] == "hint")
            {
                if (method.size() != 5 && method.size() != 6)
                    throw runtime_error("Invalid index schema");

                if (method[2] == "mrg")
                {
                    if (method[3] == "dyn")
                    {
                        double dynbase = stod(method[4]);
                        double dynratio = stod(method[5]);
                        return new tIFHINTg(boundaries, 0, dynbase, dynratio, O);
                    }
                    else if (method[3] == "static")
                    {
                        int bits = stoi(method[4]);
                        return new tIFHINTg(boundaries, bits, 0.0, 0.0, O);
                    }
                    else
                        throw runtime_error("Invalid index schema");
                }
                else
                    throw runtime_error("Invalid index schema");
            }
            else
                throw runtime_error("Invalid index schema");
        }
        else
            throw runtime_error("Invalid index schema");
    }
    else if (method[0] == "irhint")
    {
        if (method[1] == "perf")
        {
            int bits = stoi(method[2]);
            return new irHINTa(boundaries, bits, O);
        }
        else
            throw runtime_error("Invalid index schema");
    }
    else
        throw runtime_error("Invalid index schema");
}


Moveout* SynthDexOpt::new_fanout_node_hybrid(
    const IdxSchema &schema,
    const IRelation &O)
{
    if (schema.hybrid == "moveout-refine")
    {
        if (schema.fanout.size() != 2)
            throw runtime_error("Hybrid fanout must have exactly two children");

        auto boundaries = Boundaries(0, this->ostats.dict, 0, numeric_limits<Timestamp>::max());

        auto moveout_node = this->new_terminal_node(O, schema.fanout[0].method, boundaries);
        auto refinement_node = this->new_terminal_node(O, schema.fanout[1].method, boundaries);

        auto hybrid_fanout = new MoveoutRefinementHybrid(
            dynamic_cast<Refinement_ElemFreq*>(moveout_node), 
            dynamic_cast<Refinement_ElemFreq*>(refinement_node));

        return hybrid_fanout;
    }
    else
        throw runtime_error("Hybrid fanout type not supported");
}


Moveout* SynthDexOpt::new_fanout_node_refined(
    const IdxSchema &schema,
    const IRelation &O)
{
    if (schema.refine == "elemfreq")
    {
        auto refinement_fanout = new RefinementFanout_ElemFreq();
        refinement_fanout->start = 0;
        refinement_fanout->end = O.gend;

        for (auto r : schema.fanout)
        {
            ElementId elem_min = 0;
            ElementId elem_max = this->ostats.dict;
            parse_min_max(r.range, elem_min, elem_max, this->ostats.elements.back().hi_rank_log, true);

            if (elem_min == elem_max) continue;

            auto boundaries = Boundaries(elem_min, elem_max, 0, numeric_limits<Timestamp>::max());
            auto node = this->new_terminal_node(O, r.method, boundaries);
        
            refinement_fanout->refinements.push_back(
                make_tuple(boundaries, (Refinement_ElemFreq*)node));
        }

        return refinement_fanout;
    }
    else
        throw runtime_error("Refinement dimension not supported");
}


Moveout* SynthDexOpt::new_fanout_node_splitted(
    const IdxSchema &schema,
    const IRelation &O)
{
    if (schema.split == "temporal")
    {
        auto split_fanout = new SplitFanout_Temporal();

        const size_t num_partitions = schema.fanout.size();
        vector<Boundaries> boundariess(num_partitions);
        vector<IRelation> O_fragments(num_partitions);
        
        // Determine boundaries
        for (size_t i = 0; i < num_partitions; ++i)
        {
            Timestamp start = 0;
            Timestamp end = this->ostats.domain;
            parse_min_max(schema.fanout[i].range, start, end, this->ostats.domain_log, false);

            boundariess[i] = Boundaries(0, numeric_limits<ElementId>::max(), start, end+1);

            size_t fragment_size = static_cast<size_t>(
                O.size() * ((boundariess[i].time_end - boundariess[i].time_start)
                    / static_cast<double>(this->ostats.domain)));

            O_fragments[i].reserve(fragment_size);
        }

        // Fragment O and adjust to partition-local coordinates
        for (const auto& record : O)
        {
            for (size_t i = 0; i < num_partitions; ++i)
            {
                if (record.start >= boundariess[i].time_start && record.end <= boundariess[i].time_end)
                {
                    IRecord local_record(record.id, 
                        record.start - boundariess[i].time_start,
                        record.end - boundariess[i].time_start);
                    local_record.elements = record.elements;
                    O_fragments[i].push_back(local_record);
                    break;
                }
            }
        }

        // Build nodes
        split_fanout->pages.resize(num_partitions);
        
        #pragma omp parallel for schedule(dynamic)
        for (size_t i = 0; i < num_partitions; ++i)
        {
            O_fragments[i].determineDomain();
            O_fragments[i].shrink_to_fit();
            
            bool is_full_domain_single_partition = 
                (num_partitions == 1 && boundariess[i].time_start == 0 && 
                 boundariess[i].time_end > this->ostats.domain);
            
            Timestamp terminal_time_end = is_full_domain_single_partition
                ? numeric_limits<Timestamp>::max()
                : boundariess[i].time_end - boundariess[i].time_start;
            
            Boundaries terminal_boundaries(0, numeric_limits<ElementId>::max(), 0, terminal_time_end);
            
            auto node = this->new_node(O_fragments[i], schema.fanout[i], terminal_boundaries);
            split_fanout->pages[i] = make_tuple(boundariess[i], node);
        }

        return split_fanout;
    }
    else
        throw runtime_error("Dimension not supported for split");
}


Moveout* SynthDexOpt::new_fanout_node_sliced(
    const IdxSchema &schema,
    const IRelation &O)
{
    if (schema.slice == "temporal")
    {
        auto slice_fanout = new SliceFanout_Temporal();

        const size_t num_partitions = schema.fanout.size();
        vector<Boundaries> boundariess(num_partitions);
        vector<IRelation> O_fragments(num_partitions);
        
        // Determine boundaries
        for (size_t i = 0; i < num_partitions; ++i)
        {
            Timestamp start = 0;
            Timestamp end = this->ostats.domain;
            parse_min_max(schema.fanout[i].range, start, end, this->ostats.domain_log, false);

            boundariess[i] = Boundaries(0, numeric_limits<ElementId>::max(), start, end+1);

            size_t fragment_size = static_cast<size_t>(
                O.size() * ((boundariess[i].time_end - boundariess[i].time_start)
                    / static_cast<double>(this->ostats.domain)) * 1.5);

            O_fragments[i].reserve(fragment_size);
        }

        // Slice O: assign records to ALL overlapping partitions
        for (const auto& record : O)
        {
            for (size_t i = 0; i < num_partitions; ++i)
            {
                if (record.start < boundariess[i].time_end && record.end > boundariess[i].time_start)
                {
                    Timestamp local_start = max(record.start, boundariess[i].time_start) 
                        - boundariess[i].time_start;
                    Timestamp local_end = min(record.end, boundariess[i].time_end) 
                        - boundariess[i].time_start;
                    
                    IRecord local_record(record.id, local_start, local_end);
                    local_record.elements = record.elements;
                    O_fragments[i].push_back(local_record);
                }
            }
        }

        // Build nodes
        slice_fanout->pages.resize(num_partitions);
        
        #pragma omp parallel for schedule(dynamic)
        for (size_t i = 0; i < num_partitions; ++i)
        {
            O_fragments[i].determineDomain();
            O_fragments[i].shrink_to_fit();
            
            bool is_full_domain_single_partition = 
                (num_partitions == 1 && boundariess[i].time_start == 0 && 
                 boundariess[i].time_end > this->ostats.domain);
            
            Timestamp terminal_time_end = is_full_domain_single_partition
                ? numeric_limits<Timestamp>::max()
                : boundariess[i].time_end - boundariess[i].time_start;
            
            Boundaries terminal_boundaries(0, numeric_limits<ElementId>::max(), 0, terminal_time_end);
            
            auto node = this->new_node(O_fragments[i], schema.fanout[i], terminal_boundaries);
            slice_fanout->pages[i] = make_tuple(boundariess[i], node);
        }

        return slice_fanout;
    }
    else
        throw runtime_error("Dimension not supported for slice");
}


void SynthDexOpt::query(const RangeIRQuery &q, RelationId &result)
{
    auto elem = 0;
    this->index->move_out(q, elem, result);
}


void SynthDexOpt::update(const IRelation &R)
{
    this->index->update(R);
}


void SynthDexOpt::remove(const RelationId &ids)
{
    this->index->remove(ids);
}


size_t SynthDexOpt::getSize()
{
    size_t size = sizeof(*this);
    if (this->index != nullptr)
        size += this->index->getSize();
    return size;
}


string SynthDexOpt::str() const
{
    return this->index->str(0);
}
