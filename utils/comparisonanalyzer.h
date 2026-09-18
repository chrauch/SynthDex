#ifndef _COMPARISONANALYZER_H_
#define _COMPARISONANALYZER_H_

#include "indexevaluator.h"
#include <map>
#include <unordered_map>
#include <vector>

using namespace std;

struct ComparisonMeasurement
{
    string category;
    size_t query_count = 0;
    double construction_s = 0.0;
    double size_mb = 0.0;
    vector<double> median_query_times;
};

using ComparisonMeasurements = unordered_map<
    string, vector<ComparisonMeasurement>>;

class ComparisonAnalyzer : public IndexEvaluator
{
public:
    ComparisonAnalyzer(
        const ObjectDomain& domain,
        const tuple<string,vector<RangeIRQuery>>& Q,
        const OStats& Ostats,
        ComparisonMeasurements& measurements,
        const string& category);

    void run(IRIndex* idx, const iStats& Istats, const double& construction) override;

private:
    ComparisonMeasurements& measurements;
    string category;
    map<string,size_t> result_counts;
    map<string,size_t> result_xors;
};

#endif // _COMPARISONANALYZER_H_