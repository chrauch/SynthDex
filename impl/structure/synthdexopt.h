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

#ifndef SYNTHDEXOPT_H
#define SYNTHDEXOPT_H

#include "../structure/framework.h"
#include "../learning/stats.h"
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include <string>


// Standardized construction arguments for all nodes
struct NodeArgs {
    const IRelation& O;
    const Boundaries boundaries;
    const vector<string> method;
    const OStats& ostats;
    
    NodeArgs(const IRelation& o, const Boundaries& b, 
             const vector<string>& m, const OStats& os)
        : O(o), boundaries(b), method(m), ostats(os) {}
};


// Forward declarations for factory functions
class tIF;
class tIFSLICING;
class tIFSHARDING;
class tIFHINTg;
class irHINTa;

// Factory functions to create terminals from standardized args
template<typename TerminalType>
unique_ptr<TerminalType> create_terminal(const NodeArgs& args);

// Specializations declared in .cpp


// Template-based refinement fanout for compile-time optimization
template<typename... Children>
class RefinementFanout_ElemFreq_T : public Moveout
{
public:
    const tuple<pair<Boundaries, unique_ptr<Children>>...> refinements;
    const Timestamp start;
    const Timestamp end;

    template<typename... Args>
    RefinementFanout_ElemFreq_T(Timestamp s, Timestamp e, 
                                 pair<Boundaries, unique_ptr<Children>>... children)
        : refinements(move(children)...), start(s), end(e) {}

    __attribute__((hot)) inline
    void move_out(const RangeIRQuery &qo, ElementId &elem_offo, RelationId &result) override
    {
        if (qo.start > this->end) [[unlikely]] return;

        int elem_off = elem_offo;

        const ElementId elem_first = qo.elems.front();

        // Find first matching refinement, move_out, then refine remaining
        find_and_moveout<0>(qo, elem_off, result, elem_first);
    }

    inline void update(const IRelation &R) override
    {
        update_children<0>(R);
    }

    inline void remove(const RelationId &ids) override
    {
        remove_children<0>(ids);
    }

    size_t getSize() override
    {
        size_t size = sizeof(*this);
        compute_size<0>(size);
        return size;
    }

    string str(int level) const override
    {
        ostringstream oss;
        oss << "Refinement-Fanout-T (time [" << start << ".." << end << "])\n";
        apply([&](const auto&... children) {
            (..., (oss << "\t+ " << children.first.str() << "-> " 
                       << (children.second ? children.second->str(level + 1) : "null") << "\n"));
        }, refinements);
        return indent(level, oss.str());
    }

private:
    // Find first refinement where b.elem_min <= elem_first,
    // call move_out on it, then refine remaining
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void find_and_moveout(const RangeIRQuery &qo, int &elem_off, RelationId &result,
                          ElementId elem_first)
    {
        if constexpr (I < sizeof...(Children))
        {
            const auto& child = get<I>(refinements);
            const auto &b = child.first;
            
            if (b.elem_min <= elem_first)
            {
                // First matching refinement: call move_out
                child.second->move_out(qo, elem_off, result);
                if (result.empty() || elem_off >= static_cast<int>(qo.elems.size())) return;
                // Refine all remaining children
                refine_remaining<I + 1>(qo, elem_off, result);
                return;
            }
            
            // Skip this refinement (elem_min > elem_first), try next
            find_and_moveout<I + 1>(qo, elem_off, result, elem_first);
        }
    }

    // Phase 3: For remaining refinements, call refine() if in range
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void refine_remaining(const RangeIRQuery &qo, int &elem_off, RelationId &result)
    {
        if constexpr (I < sizeof...(Children))
        {
            const auto& child = get<I>(refinements);
            const auto &b = child.first;
            
            if (b.elem_min <= qo.elems[elem_off])
            {
                // Use compile-time type information instead of dynamic_cast
                using ChildType = tuple_element_t<I, tuple<Children...>>;
                if constexpr (is_base_of_v<Refinement_ElemFreq, ChildType>)
                {
                    static_cast<Refinement_ElemFreq*>(child.second.get())->refine(qo, elem_off, result);
                }
                if (elem_off == static_cast<int>(qo.elems.size()) || result.empty()) return;
            }
            
            refine_remaining<I + 1>(qo, elem_off, result);
        }
    }

    // Optimized update using compile-time recursion
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void update_children(const IRelation &R) noexcept
    {
        if constexpr (I < sizeof...(Children))
        {
            get<I>(refinements).second->update(R);
            update_children<I + 1>(R);
        }
    }

    // Optimized remove using compile-time recursion
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void remove_children(const RelationId &ids) noexcept
    {
        if constexpr (I < sizeof...(Children))
        {
            get<I>(refinements).second->remove(ids);
            remove_children<I + 1>(ids);
        }
    }

    // Optimized size computation
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void compute_size(size_t &size) const
    {
        if constexpr (I < sizeof...(Children))
        {
            const auto& child = get<I>(refinements);
            size += sizeof(child) + (child.second ? child.second->getSize() : 0);
            compute_size<I + 1>(size);
        }
    }
};


// Template-based split/slice fanout
template<typename... Children>
class SplitFanout_Temporal_T : public Moveout
{
public:
    const tuple<pair<Boundaries, unique_ptr<Children>>...> pages;

    template<typename... Args>
    SplitFanout_Temporal_T(pair<Boundaries, unique_ptr<Children>>... children)
        : pages(move(children)...) {}

    __attribute__((hot)) inline
    void move_out(const RangeIRQuery &q, ElementId &elem_off, RelationId &result) override
    {
        query_pages<0>(q, elem_off, result);
    }

    inline void update(const IRelation &R) override
    {
        update_pages<0>(R);
    }

    inline void remove(const RelationId &ids) override
    {
        remove_pages<0>(ids);
    }

    size_t getSize() override
    {
        size_t size = sizeof(*this);
        compute_page_size<0>(size);
        return size;
    }

    string str(int level) const override
    {
        ostringstream oss;
        oss << "Split-Fanout-T\n";
        apply([&](const auto&... children) {
            (..., (oss << "\t+ " << children.first.str() << "-> " 
                       << (children.second ? children.second->str(level + 1) : "null") << "\n"));
        }, pages);
        return indent(level, oss.str());
    }

private:
    // Compile-time recursion for page queries
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void query_pages(const RangeIRQuery &q, ElementId &elem_off, RelationId &result)
    {
        if constexpr (I < sizeof...(Children))
        {
            const auto& child = get<I>(pages);
            const auto &b = child.first;
            
            // Temporal range check with short-circuit
            if ((q.start < b.time_end) && (q.end > b.time_start)) [[likely]]
            {
                const Timestamp local_start = max(q.start, b.time_start) - b.time_start;
                const Timestamp local_end = min(q.end, b.time_end) - b.time_start;
                
                // Note: Query copy includes vector - consider ref-counted impl if this becomes bottleneck
                RangeIRQuery local_q = q;
                local_q.start = local_start;
                local_q.end = local_end;
                child.second->move_out(local_q, elem_off, result);
            }
            
            // Prefetch next page only for larger tuples
            if constexpr ((I + 1 < sizeof...(Children)) && (sizeof...(Children) >= 4))
                __builtin_prefetch(&get<I + 1>(pages), 0, 3);
            
            query_pages<I + 1>(q, elem_off, result);
        }
    }

    // Optimized update using compile-time recursion
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void update_pages(const IRelation &R) noexcept
    {
        if constexpr (I < sizeof...(Children))
        {
            get<I>(pages).second->update(R);
            update_pages<I + 1>(R);
        }
    }

    // Optimized remove using compile-time recursion
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void remove_pages(const RelationId &ids) noexcept
    {
        if constexpr (I < sizeof...(Children))
        {
            get<I>(pages).second->remove(ids);
            remove_pages<I + 1>(ids);
        }
    }

    // Optimized size computation
    template<size_t I = 0>
    __attribute__((always_inline)) inline
    void compute_page_size(size_t &size) const
    {
        if constexpr (I < sizeof...(Children))
        {
            const auto& child = get<I>(pages);
            size += sizeof(child) + (child.second ? child.second->getSize() : 0);
            compute_page_size<I + 1>(size);
        }
    }
};


// Template for slice (alias split, implementation is similar for our patterns)
template<typename... Children>
using SliceFanout_Temporal_T = SplitFanout_Temporal_T<Children...>;


/**
 * Optimized SynthDex using template-based construction for compile-time optimization.
 * Pattern-matches common index configurations to eliminate virtual calls.
 */
class SynthDexOpt : public IRIndex
{
private:
    IdxSchema schema;
    OStats ostats;
    unique_ptr<Moveout> index;
    
    // Construction helpers
    Moveout* construct(const IRelation &O, const IdxSchema &schema, const Boundaries &boundaries);
    Moveout* try_pattern_match(const IRelation &O, const IdxSchema &schema, const Boundaries &boundaries);
    Moveout* new_node(const IRelation &O, const IdxSchema &schema, const Boundaries &boundaries);
    Moveout* new_terminal_node(const IRelation &O, const vector<string> &method, const Boundaries &boundaries);
    Moveout* new_fanout_node(const IRelation &O, const IdxSchema &schema, const Boundaries &boundaries);
    Moveout* new_fanout_node_hybrid(const IdxSchema &schema, const IRelation &O);
    Moveout* new_fanout_node_refined(const IdxSchema &schema, const IRelation &O);
    Moveout* new_fanout_node_splitted(const IdxSchema &schema, const IRelation &O);
    Moveout* new_fanout_node_sliced(const IdxSchema &schema, const IRelation &O);
    
    static void parse_min_max(const string &range, int &min_v, int &max_v, 
                              const double &max_log, const bool &log_scale);
    static string method_signature(const vector<string> &method);

public:
    SynthDexOpt(const IRelation &O, const IdxSchema &schema, const OStats &ostats);
    
    void query(const RangeIRQuery &q, RelationId &result) override;
    void update(const IRelation &R) override;
    void remove(const RelationId &ids) override;
    size_t getSize() override;
    string str() const;
    
    ~SynthDexOpt() = default;
};

#endif // SYNTHDEXOPT_H
