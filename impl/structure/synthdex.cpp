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

#include "synthdex.h"
#include "../utils/parsing.h"
#include "../terminal/tif.h"
#include "../terminal/irhint.h"
#include "../terminal/tifhintg.h"
#include "../terminal/tifslicing.h"
#include "../terminal/tifsharding.h"
#include "../structure/idxschemaserializer.h"
#include "../learning/statsserializer.h"
#include <omp.h>


SynthDex::SynthDex(
    const IRelation &O,
    const IdxSchema &schema,
    const OStats &ostats)
    : schema(schema),
      ostats(ostats),
      index(this->construct(O, schema, Boundaries())),
      IRIndex() { }


Moveout* SynthDex::construct(
    const IRelation &O,
    const IdxSchema &schema,
    const Boundaries &boundaries)
{
    Moveout* index = this->new_node(O, schema, boundaries);
    
    return index;
}

Moveout* SynthDex::new_node(
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


void parse_min_max(
    const string &range, int &min_v, int &max_v, const double &max_log, const bool &log_scale)
{
    size_t pos = range.find('-');
    if (pos != string::npos)
    {
        double min_rel = stod(range.substr(0, pos));
        double max_rel = stod(range.substr(pos + 1));

        if (log_scale)
        {
            int min_calc = min_rel == 0 ? 0
                : (int)ceil(pow(10, min_rel * max_log));
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


Moveout* SynthDex::new_fanout_node(
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


Moveout* SynthDex::new_terminal_node(
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


Moveout* SynthDex::new_fanout_node_hybrid(
    const IdxSchema &schema,
    const IRelation &O)
{
    if (schema.hybrid == "moveout-refine")
    {
        if (schema.fanout.size() != 2)
            throw runtime_error(
                "Hybrid fanout must have exactly two children");

        auto boundaries = Boundaries(
            0, this->ostats.dict, 
            0, numeric_limits<Timestamp>::max());

        auto moveout_node = this->new_terminal_node(
            O, schema.fanout[0].method, boundaries);

        auto refinement_node = this->new_terminal_node(
            O, schema.fanout[1].method, boundaries);

        auto hybrid_fanout = new MoveoutRefinementHybrid(
            dynamic_cast<Refinement_ElemFreq*>(moveout_node), 
            dynamic_cast<Refinement_ElemFreq*>(refinement_node));

        return hybrid_fanout;
    }
    else
        throw runtime_error("Hybrid fanout type not supported");
}


Moveout* SynthDex::new_fanout_node_refined(
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
            parse_min_max(r.range, elem_min, elem_max,
                this->ostats.elements.back().hi_rank_log, true);

            if (elem_min == elem_max) continue;

            auto boundaries = Boundaries(
                elem_min, elem_max, 0, numeric_limits<Timestamp>::max());

            auto node = this->new_terminal_node(
                O, r.method, boundaries);
        
            refinement_fanout->refinements.push_back(
                make_tuple(boundaries, (Refinement_ElemFreq*)node));
        }

        return refinement_fanout;
    }
    else
        throw runtime_error("Refinement dimension not supported");
}


Moveout* SynthDex::new_fanout_node_splitted(
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
            parse_min_max(schema.fanout[i].range, start, end,
                this->ostats.domain_log, false);

            boundariess[i] = Boundaries(
                0, numeric_limits<ElementId>::max(), start, end+1);

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
                if (record.start >= boundariess[i].time_start
                    && record.end <= boundariess[i].time_end)
                {
                    // Adjust record to partition-local coordinates
                    IRecord local_record(
                        record.id,
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
            
            // Special case: single partition covering full domain should match unwrapped case
            bool is_full_domain_single_partition = 
                (num_partitions == 1 && 
                 boundariess[i].time_start == 0 && 
                 boundariess[i].time_end > this->ostats.domain);
            
            Timestamp terminal_time_end = is_full_domain_single_partition
                ? numeric_limits<Timestamp>::max()  // Match unwrapped terminal
                : boundariess[i].time_end - boundariess[i].time_start;  // Partition size
            
            Boundaries terminal_boundaries(
                0, numeric_limits<ElementId>::max(), 
                0, terminal_time_end);
            
            auto node = this->new_node(
                O_fragments[i], schema.fanout[i], terminal_boundaries);
            split_fanout->pages[i] = make_tuple(boundariess[i], node);
        }

        return split_fanout;
    }
    else
        throw runtime_error("Dimension not supported for split");
}


Moveout* SynthDex::new_fanout_node_sliced(
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
            parse_min_max(schema.fanout[i].range, start, end,
                this->ostats.domain_log, false);

            boundariess[i] = Boundaries(
                0, numeric_limits<ElementId>::max(), start, end+1);

            // Estimate size conservatively (records can be in multiple partitions)
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
                // Check if record overlaps with this partition
                // Record [record.start, record.end) overlaps [time_start, time_end)
                if (record.start < boundariess[i].time_end 
                    && record.end > boundariess[i].time_start)
                {
                    // Clip record to partition boundaries and adjust to local coordinates
                    Timestamp local_start = max(record.start, boundariess[i].time_start) 
                        - boundariess[i].time_start;
                    Timestamp local_end = min(record.end, boundariess[i].time_end) 
                        - boundariess[i].time_start;
                    
                    IRecord local_record(
                        record.id,
                        local_start,
                        local_end);
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
            
            // Special case: single partition covering full domain should match unwrapped case
            bool is_full_domain_single_partition = 
                (num_partitions == 1 && 
                 boundariess[i].time_start == 0 && 
                 boundariess[i].time_end > this->ostats.domain);
            
            Timestamp terminal_time_end = is_full_domain_single_partition
                ? numeric_limits<Timestamp>::max()  // Match unwrapped terminal
                : boundariess[i].time_end - boundariess[i].time_start;  // Partition size
            
            Boundaries terminal_boundaries(
                0, numeric_limits<ElementId>::max(), 
                0, terminal_time_end);
            
            auto node = this->new_node(
                O_fragments[i], schema.fanout[i], terminal_boundaries);
            slice_fanout->pages[i] = make_tuple(boundariess[i], node);
        }

        return slice_fanout;
    }
    else
        throw runtime_error("Dimension not supported for slice");
}


// Querying
void SynthDex::query(const RangeIRQuery &q, RelationId &result)
{
    auto elem = 0;
    this->index->move_out(q, elem, result);
}


// Updating
void SynthDex::update(const IRelation &R)
{
    this->index->update(R);
}


void SynthDex::remove(const vector<bool> &idsToDelete)
{
    this->index->remove(idsToDelete);
}


void SynthDex::softdelete(const vector<bool> &idsToDelete)
{
    this->index->softdelete(idsToDelete);
}


size_t SynthDex::getSize()
{
    size_t size = 0;
    
    // Member variables
    //size += sizeof(IdxSchema);  // schema
    //size += sizeof(OStats);     // ostats
    size += sizeof(Moveout*);   // index pointer
    
    // The index structure itself
    if (this->index != nullptr)
        size += this->index->getSize();
    
    return size;
}


SynthDex::~SynthDex()
{
    delete this->index;
}


string SynthDex::str() const
{
    return this->index->str(0);
}