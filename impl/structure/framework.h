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


#ifndef _FRAMEWORK_H_
#define _FRAMEWORK_H_

#include "../utils/global.h"
#include "../containers/relations.h"
#include "idxschema.h"


struct Boundaries
{
    Timestamp time_start = 0;
    Timestamp time_end = numeric_limits<Timestamp>::max();
    ElementId elem_min = 0;
    ElementId elem_max = numeric_limits<ElementId>::max();

    Boundaries() = default;

    Boundaries(
            const ElementId& elem_min,
            const ElementId& elem_max,
            const Timestamp& time_start,
            const Timestamp& time_end)
        : time_start(time_start),
          time_end(time_end),
          elem_min(elem_min),
          elem_max(elem_max)
    {
    }

    string str() const 
    {
        ostringstream oss;

        if (elem_min != 0 || elem_max != 
            numeric_limits<ElementId>::max())
            {
                oss << "elements [" << elem_min << ".." << elem_max << ") ";
            }

        if (time_start != 0 || time_end != 
            numeric_limits<Timestamp>::max())
            {
                oss << "time [" << time_start << ".." << time_end << ") ";
            }

        return oss.str();
    }
};


class Moveout 
{
public:
    virtual ~Moveout() {}
    virtual void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &result) = 0;

    virtual void update(const IRelation &R) = 0;

    virtual void remove(const vector<bool> &idsToDelete) = 0;

    virtual void softdelete(const vector<bool> &idsToDelete) = 0;

    virtual size_t getSize() { return 0; };

    virtual string str(int level) const { return indent(level, "Moveout"); };
};


class RefinementFanout : public Moveout 
{
};


class SplitFanout : public Moveout 
{
};


class SplitFanout_Temporal : public Moveout
{
public:
    vector<tuple<Boundaries, Moveout *>> pages;

    void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &result);

    void update(const IRelation &R) override;

    void remove(const vector<bool> &idsToDelete) override;

    void softdelete(const vector<bool> &idsToDelete) override;

    size_t getSize() override
    {
        size_t size = sizeof(*this);
        size += pages.size() * sizeof(tuple<Boundaries, Moveout*>);
        for (const auto &[b, m] : pages)
        {
            if (m != nullptr)
                size += m->getSize();
        }
        return size;
    }

    string str(int level) const override
    {
        ostringstream oss;
        oss << "Split-Fanout\n";
        for (size_t i = 0; i < pages.size(); ++i)
        {
            const auto &[b, m] = pages[i];
            oss << "\t+ " << b.str() << "-> "
                << (m ? m->str(level + 1) : "null");
            if (i + 1 < pages.size())
                oss << ", \n";
        }
        return indent(level, oss.str());
    }

    ~SplitFanout_Temporal();
};


class SliceFanout_Temporal : public Moveout
{
public:
    vector<tuple<Boundaries, Moveout *>> pages;

    void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &result);

    void update(const IRelation &R) override;

    void remove(const vector<bool> &idsToDelete) override;

    void softdelete(const vector<bool> &idsToDelete) override;

    size_t getSize() override
    {
        size_t size = sizeof(*this);
        size += pages.size() * sizeof(tuple<Boundaries, Moveout*>);
        for (const auto &[b, m] : pages)
        {
            if (m != nullptr)
                size += m->getSize();
        }
        return size;
    }

    string str(int level) const override
    {
        ostringstream oss;
        oss << "Slice-Fanout\n";
        for (size_t i = 0; i < pages.size(); ++i)
        {
            const auto &[b, m] = pages[i];
            oss << "\t+ " << b.str() << "-> "
                << (m ? m->str(level + 1) : "null");
            if (i + 1 < pages.size())
                oss << ", \n";
        }
        return indent(level, oss.str());
    }

    ~SliceFanout_Temporal();
};


class Refinement_ElemFreq : public Moveout
{
public:
    virtual void refine(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &candidates) {};
};


class MoveoutRefinementHybrid : public Moveout 
{
public:
    Refinement_ElemFreq* moveout_node;  // Used for initial moveout
    Refinement_ElemFreq* refine_node;   // Used for refinement

    MoveoutRefinementHybrid(Refinement_ElemFreq* moveout, Refinement_ElemFreq* refine)
        : moveout_node(moveout), refine_node(refine) {}

    void move_out(
        const RangeIRQuery &q,
        ElementId &elem_off,
        RelationId &result) override;

    void update(const IRelation &R) override;

    void remove(const vector<bool> &idsToDelete) override;

    void softdelete(const vector<bool> &idsToDelete) override;

    size_t getSize() override
    {
        size_t size = 0;
        size += sizeof(Refinement_ElemFreq*) * 2;  // Two pointers
        
        if (moveout_node != nullptr)
            size += moveout_node->getSize();
        if (refine_node != nullptr)
            size += refine_node->getSize();
        
        return size;
    }

    string str(int level) const override
    {
        ostringstream oss;
        oss << "Moveout-Refinement-Hybrid\n";
        oss << "\t+ Moveout: " << (moveout_node ? moveout_node->str(level + 1) : "null") << "\n";
        oss << "\t+ Refine: " << (refine_node ? refine_node->str(level + 1) : "null");
        return indent(level, oss.str());
    }

    ~MoveoutRefinementHybrid();
};


class RefinementFanout_ElemFreq : public RefinementFanout
{
public:
    vector<tuple<Boundaries,Refinement_ElemFreq*>> refinements;

    void move_out(
        const RangeIRQuery &qo, 
        ElementId &elem_off,
        RelationId &result);

    void update(const IRelation &R) override;

    void remove(const vector<bool> &idsToDelete) override;

    void softdelete(const vector<bool> &idsToDelete) override;

    Timestamp start;
    Timestamp end;

    size_t getSize() override
    {
        size_t size = 0;
        size += sizeof(Timestamp) * 2;  // start, end
        size += sizeof(vector<tuple<Boundaries, Refinement_ElemFreq*>>);  // Vector structure
        size += refinements.size() * sizeof(tuple<Boundaries, Refinement_ElemFreq*>);  // Vector data
        
        // Size of each refinement's content
        for (const auto &[b, r] : refinements)
        {
            size += sizeof(Boundaries);
            if (r != nullptr)
                size += r->getSize();
        }
        
        return size;
    }

    string str(int level) const override
    {
        ostringstream oss;
        oss << "Refinement-Fanout";
        oss << " (time [" << start << ".." << end << "])\n";
        for (size_t i = 0; i < refinements.size(); ++i)
        {
            const auto &[b, r] = refinements[i];
            oss << "\t+ " << b.str() << "-> " 
                << (r ? r->str(level + 1) : "null");
            if (i + 1 < refinements.size())
                oss << ", \n";
        }
        return indent(level, oss.str());
    }

    ~RefinementFanout_ElemFreq();
};



#endif // _FRAMEWORK_H_
