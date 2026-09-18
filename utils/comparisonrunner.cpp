#include "comparisonrunner.h"
#include "controller.h"
#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>

using namespace std;

namespace
{
struct ComparisonTableAccumulator
{
    explicit ComparisonTableAccumulator(size_t size)
        : sums(size, vector<double>(size, 0.0)),
          counts(size, vector<size_t>(size, 0))
    {
    }

    void add(size_t row, size_t column, double value)
    {
        sums[row][column] += value;
        counts[row][column]++;
    }

    vector<vector<double>> averages() const
    {
        vector<vector<double>> result = sums;
        for (size_t row = 0; row < sums.size(); ++row)
            for (size_t column = 0; column < sums.size(); ++column)
                if (counts[row][column] > 0)
                    result[row][column] /= counts[row][column];
                else
                    result[row][column]
                        = numeric_limits<double>::quiet_NaN();
        return result;
    }

    vector<vector<double>> sums;
    vector<vector<size_t>> counts;
};

using ComparisonTable = vector<vector<double>>;

struct IntermediateTable
{
    string collection_name;
    vector<string> workloads;
    ComparisonTable values;
};

IntermediateTable read_intermediate_table(
    const string& path,
    bool ratio_table = false)
{
    ifstream fs(path);
    if (!fs.is_open())
        throw runtime_error("Error opening comparison intermediate: " + path);

    IntermediateTable result;
    string line;
    bool header_read = false;
    while (getline(fs, line))
    {
        if (line.rfind("# Collection: ", 0) == 0)
        {
            result.collection_name = line.substr(14);
            continue;
        }
        if (line.empty() || line[0] == '#') continue;

        string cell;
        stringstream row_stream(line);
        vector<string> cells;
        while (getline(row_stream, cell, '\t'))
            cells.push_back(cell);
        if (!line.empty() && line.back() == '\t')
            cells.push_back("");

        if (!header_read)
        {
            if (cells.empty() || !cells[0].empty() || cells.size() < 2)
                throw runtime_error("Invalid comparison intermediate header: " + path);
            result.workloads.assign(cells.begin() + 1, cells.end());
            header_read = true;
            continue;
        }

        if (cells.size() != result.workloads.size() + 1)
            throw runtime_error("Invalid comparison intermediate row: " + path);
        const size_t row_idx = result.values.size();
        if (row_idx >= result.workloads.size()
            || cells[0] != result.workloads[row_idx])
            throw runtime_error("Comparison intermediate row mismatch: " + path);
        vector<double> values;
        values.reserve(result.workloads.size());
        for (size_t idx = 0; idx < result.workloads.size(); ++idx)
        {
            string value = cells[idx + 1];
            if (value.empty())
            {
                values.push_back(numeric_limits<double>::quiet_NaN());
                continue;
            }
            if (value.back() == '%')
                value.pop_back();

            const size_t separator = value.find(':');
            if (separator == string::npos)
            {
                values.push_back(stod(value));
                continue;
            }

            if (!ratio_table)
                throw runtime_error(
                    "Invalid ratio cell in comparison intermediate: " + path);

            const double column_ratio = stod(value.substr(0, separator));
            const double row_ratio = stod(value.substr(separator + 1));
            values.push_back(column_ratio);
            if (result.values.empty() && idx > 0)
                values[0] = row_ratio;
        }
        result.values.push_back(move(values));
    }

    if (result.collection_name.empty()
        || result.workloads.empty()
        || result.values.size() != result.workloads.size())
        throw runtime_error("Incomplete comparison intermediate: " + path);
    return result;
}

string intermediate_tag(const string& filename)
{
    const string marker = ".comparison-intermediate.";
    const size_t marker_pos = filename.find(marker);
    if (marker_pos == string::npos
        || filename.size() <= marker_pos + marker.size() + 4
        || filename.substr(filename.size() - 4) != ".csv")
        return "";
    return filename.substr(
        marker_pos + marker.size(),
        filename.size() - marker_pos - marker.size() - 4);
}

string format_ratio(double value)
{
    ostringstream text;
    text << fixed << setprecision(2) << value;
    string result = text.str();
    while (result.size() > 1 && result.back() == '0')
        result.pop_back();
    if (!result.empty() && result.back() == '.')
        result.pop_back();
    return result;
}
}


ComparisonRunner::ComparisonRunner(Controller &controller)
    : controller(controller)
{
}


vector<string> ComparisonRunner::split_pipe(const string &spec)
{
    vector<string> parts;
    stringstream ss(spec);
    string part;
    while (getline(ss, part, '|'))
    {
        if (!part.empty()) parts.push_back(part);
    }
    return parts;
}


vector<string> ComparisonRunner::split_indices(const string &spec)
{
    auto indices = split_pipe(spec);
    if (indices.size() < 2)
        throw invalid_argument("compare requires at least two indexes");
    return indices;
}


vector<string> ComparisonRunner::expand_workloads(const string &spec)
{
    auto workloads = split_pipe(spec);
    if (workloads.empty())
        throw invalid_argument("compare requires at least one workload");

    for (const auto &workload : workloads)
        if (workload[0] != '%')
            throw invalid_argument(
                "compare pairwise workloads require %WORKLOAD specifications");

    set<string> unique_workloads(workloads.begin(), workloads.end());
    if (unique_workloads.size() != workloads.size())
        throw invalid_argument("compare workload specifications must be unique");

    return workloads;
}


string ComparisonRunner::join_pipe(const vector<string> &parts)
{
    string result;
    for (const auto &part : parts)
    {
        if (!result.empty()) result += '|';
        result += part;
    }
    return result;
}


void ComparisonRunner::run()
{
    split_indices(this->controller.cmd.idxschema);
    auto workloads = expand_workloads(this->controller.cmd.file_Q);
    const string workload_spec = join_pipe(workloads);
    vector<string> workload_names;
    workload_names.reserve(workloads.size());
    for (const auto& workload : workloads)
        workload_names.push_back(workload.substr(1));

    auto get_measurements = [&](
        const ComparisonMeasurements& measurements,
        const string& raw_workload,
        const string& category = "")
    {
        vector<double> samples;
        for (const auto &[q_name, index_measurements] : measurements)
        {
            auto pattern = this->controller.Q_patterns.find(q_name);
            if (pattern == this->controller.Q_patterns.end()
                || pattern->second != raw_workload)
                continue;

            for (const auto& measurement : index_measurements)
            {
                if (!category.empty() && measurement.category != category)
                    continue;
                const double median_query_time = accumulate(
                    measurement.median_query_times.begin(),
                    measurement.median_query_times.end(), 0.0);
                if (median_query_time > 0.0)
                    samples.push_back(
                        measurement.query_count / median_query_time);
            }
        }
        return samples;
    };

    struct BestMeasurement
    {
        double throughput = 0.0;
        double construction_s = 0.0;
        double size_mb = 0.0;
    };

    auto get_best_measurement = [&] (
        const ComparisonMeasurements& measurements,
        const string& raw_workload,
        const string& category = "") -> optional<BestMeasurement>
    {
        optional<BestMeasurement> best;
        for (const auto &[q_name, index_measurements] : measurements)
        {
            auto pattern = this->controller.Q_patterns.find(q_name);
            if (pattern == this->controller.Q_patterns.end()
                || pattern->second != raw_workload)
                continue;

            for (const auto& measurement : index_measurements)
            {
                if (!category.empty() && measurement.category != category)
                    continue;
                const double median_query_time = accumulate(
                    measurement.median_query_times.begin(),
                    measurement.median_query_times.end(), 0.0);
                if (median_query_time <= 0.0)
                    continue;

                const double throughput = measurement.query_count
                    / median_query_time;
                if (!best || throughput > best->throughput)
                {
                    best = BestMeasurement{
                        throughput,
                        measurement.construction_s,
                        measurement.size_mb};
                }
            }
        }
        return best;
    };

    const bool generated_objects = this->controller.cmd.file_O.empty();
    const int object_count = generated_objects ? Cfg::get<int>("o.num") : 1;
    for (int object_idx = 0; object_idx < object_count; ++object_idx)
    {
        if (generated_objects)
            this->controller.generate_O(object_idx);
        else
            this->controller.load_O();
        this->controller.Q.clear();
        this->controller.Q_patterns.clear();
        this->controller.load_Q(workload_spec);

        bool has_empty_workload = false;
        for (size_t workload_idx = 0; workload_idx < workloads.size(); ++workload_idx)
        {
            const string raw_workload = workloads[workload_idx].substr(1);
            bool has_queries = false;
            for (const auto& Qx : this->controller.Q)
            {
                auto pattern = this->controller.Q_patterns.find(get<0>(Qx));
                if (pattern != this->controller.Q_patterns.end()
                    && pattern->second == raw_workload
                    && !get<1>(Qx).empty())
                {
                    has_queries = true;
                    break;
                }
            }

            if (!has_queries)
            {
                Log::w(0, "Skipping collection",
                    this->controller.Ostats.name
                    + ": workload has no queries " + workloads[workload_idx]);
                has_empty_workload = true;
            }
        }

        if (has_empty_workload)
        {
            Log::w(0, "Collection discarded",
                this->controller.Ostats.name
                + ": cannot derive complete workload ratios");
            continue;
        }

        ComparisonTableAccumulator average_all_indices(workloads.size());
        ComparisonTableAccumulator winner(workloads.size());
        ComparisonTableAccumulator best_synthdex(workloads.size());
        ComparisonTableAccumulator best_manual(workloads.size());
        ComparisonTableAccumulator best_synthdex_time(workloads.size());
        ComparisonTableAccumulator best_synthdex_size(workloads.size());
        ComparisonTableAccumulator best_manual_time(workloads.size());
        ComparisonTableAccumulator best_manual_size(workloads.size());

        ComparisonMeasurements measurements;
        this->controller.execute(&measurements);

        vector<double> averages;
        averages.reserve(workloads.size());

        Log::w(0, "Comparison workload calibration");
        bool collection_valid = true;
        for (size_t workload_idx = 0; workload_idx < workloads.size(); ++workload_idx)
        {
            const auto& workload = workloads[workload_idx];
            const string raw_workload = workload.substr(1);
            vector<double> all_samples = get_measurements(
                measurements, raw_workload);
            vector<double> manual_samples = get_measurements(
                measurements, raw_workload, "manual");

            if (manual_samples.empty())
            {
                Log::w(0, "Skipping collection",
                    this->controller.Ostats.name + ": no manual throughput measurements for workload "
                    + workload);
                collection_valid = false;
                break;
            }

            if (all_samples.empty())
            {
                Log::w(0, "Skipping collection",
                    this->controller.Ostats.name + ": no throughput measurements for workload "
                    + workload);
                collection_valid = false;
                break;
            }

            const double sum = accumulate(
                manual_samples.begin(), manual_samples.end(), 0.0);
            const double average = sum / manual_samples.size();
            averages.push_back(average);

            const auto synthetic_samples = get_measurements(
                measurements, raw_workload, "synthetic");
            const auto manual_best = get_best_measurement(
                measurements, raw_workload, "manual");
            const auto synthetic_best = get_best_measurement(
                measurements, raw_workload, "synthetic");
            const double all_average = accumulate(
                all_samples.begin(), all_samples.end(), 0.0)
                / all_samples.size();
            average_all_indices.add(workload_idx, workload_idx, all_average);
            winner.add(workload_idx, workload_idx,
                *max_element(all_samples.begin(), all_samples.end()));
            best_manual.add(workload_idx, workload_idx,
                *max_element(manual_samples.begin(), manual_samples.end()));
            if (manual_best)
            {
                best_manual_time.add(workload_idx, workload_idx,
                    manual_best->construction_s);
                best_manual_size.add(workload_idx, workload_idx,
                    manual_best->size_mb);
            }
            if (synthetic_best)
            {
                best_synthdex.add(workload_idx, workload_idx,
                    synthetic_best->throughput);
                best_synthdex_time.add(workload_idx, workload_idx,
                    synthetic_best->construction_s);
                best_synthdex_size.add(workload_idx, workload_idx,
                    synthetic_best->size_mb);
            }

            ostringstream sample_text;
            sample_text << fixed << setprecision(3);
            for (size_t sample_idx = 0; sample_idx < manual_samples.size(); ++sample_idx)
            {
                if (sample_idx > 0) sample_text << ", ";
                sample_text << manual_samples[sample_idx];
            }

            ostringstream average_text;
            average_text << fixed << setprecision(3) << average;
            Log::w(1, "Throughput samples [q/s]",
                workload + ": [" + sample_text.str() + "]");
            Log::w(1, "Average throughput [q/s]",
                workload + ": (" + sample_text.str() + ") / "
                + to_string(manual_samples.size()) + " = " + average_text.str());
        }

        if (!collection_valid)
        {
            Log::w(0, "Collection discarded",
                this->controller.Ostats.name
                + ": workload calibration is incomplete, so ratios and mixes are unavailable");
            continue;
        }

        const double slowest = *min_element(averages.begin(), averages.end());
        if (slowest <= 0.0)
            throw runtime_error("Cannot derive workload ratios from zero throughput");

        vector<int> ratios;
        ratios.reserve(averages.size());
        for (size_t workload_idx = 0; workload_idx < averages.size(); ++workload_idx)
        {
            const double normalized = averages[workload_idx] / slowest;
            const int ratio = max(1, (int)lround(normalized * 100.0));
            ratios.push_back(ratio);

            ostringstream normalized_text;
            normalized_text << fixed << setprecision(3) << normalized;
            Log::w(1, "Normalized workload weight",
                workloads[workload_idx] + ": " + normalized_text.str()
                + " (average throughput / slowest throughput)");
        }

        vector<string> combined_workloads;
        vector<string> skewed_combined_workloads;
        for (size_t row = 1; row < workloads.size(); ++row)
        {
            for (size_t column = 0; column < row; ++column)
            {
                const string first = to_string(ratios[column])
                    + workloads[column].substr(1);
                const string second = to_string(ratios[row])
                    + workloads[row].substr(1);
                const string combined =
                    "%" + first + "+" + second;
                combined_workloads.push_back(combined);

                const string skewed_combined =
                    "%" + first + "{ext.skew=[0-50]}+"
                    + second + "{ext.skew=[50-100]}";
                skewed_combined_workloads.push_back(skewed_combined);

                Log::w(1, "Derived workload ratio",
                    workloads[column] + " + " + workloads[row] + " -> "
                    + to_string(ratios[column]) + ":" + to_string(ratios[row])
                    + " (nearest hundredth-based integer weight from average-throughput / "
                    + to_string(slowest) + ") => " + combined);
                Log::w(1, "Derived skewed workload ratio",
                    workloads[column] + " + " + workloads[row] + " -> "
                    + to_string(ratios[column]) + ":" + to_string(ratios[row])
                    + " (first workload skew [0-50], second workload skew [50-100]) => "
                    + skewed_combined);
            }
        }

        if (!combined_workloads.empty())
        {
            vector<string> all_combined_workloads = combined_workloads;
            all_combined_workloads.insert(all_combined_workloads.end(),
                skewed_combined_workloads.begin(), skewed_combined_workloads.end());
            this->controller.O = Persistence::read_O_dat(
                this->controller.execution_O_file);
            this->controller.Q.clear();
            this->controller.Q_patterns.clear();
            this->controller.load_Q(join_pipe(all_combined_workloads));
            ComparisonMeasurements mix_measurements;
            this->controller.execute(&mix_measurements);

            auto record_mix = [&](const string& mix, size_t target_row,
                size_t target_column)
            {
                const string raw_mix = mix.substr(1);
                const auto all_samples = get_measurements(
                    mix_measurements, raw_mix);
                if (all_samples.empty())
                    throw runtime_error(
                        "No throughput measurements for workload mix: " + mix);

                const auto manual_samples = get_measurements(
                    mix_measurements, raw_mix, "manual");
                const auto manual_best = get_best_measurement(
                    mix_measurements, raw_mix, "manual");
                const auto synthetic_best = get_best_measurement(
                    mix_measurements, raw_mix, "synthetic");
                if (manual_samples.empty())
                    throw runtime_error(
                        "No manual throughput measurements for workload mix: " + mix);

                const double all_average = accumulate(
                    all_samples.begin(), all_samples.end(), 0.0)
                    / all_samples.size();
                average_all_indices.add(target_row, target_column, all_average);
                winner.add(target_row, target_column,
                    *max_element(all_samples.begin(), all_samples.end()));
                best_manual.add(target_row, target_column,
                    *max_element(manual_samples.begin(), manual_samples.end()));
                if (manual_best)
                {
                    best_manual_time.add(target_row, target_column,
                        manual_best->construction_s);
                    best_manual_size.add(target_row, target_column,
                        manual_best->size_mb);
                }
                if (synthetic_best)
                {
                    best_synthdex.add(target_row, target_column,
                        synthetic_best->throughput);
                    best_synthdex_time.add(target_row, target_column,
                        synthetic_best->construction_s);
                    best_synthdex_size.add(target_row, target_column,
                        synthetic_best->size_mb);
                }
            };

            size_t combined_idx = 0;
            for (size_t row = 1; row < workloads.size(); ++row)
            {
                for (size_t column = 0; column < row; ++column)
                    record_mix(combined_workloads[combined_idx++], row, column);
            }

            size_t skewed_idx = 0;
            for (size_t row = 1; row < workloads.size(); ++row)
            {
                for (size_t column = 0; column < row; ++column)
                    record_mix(skewed_combined_workloads[skewed_idx++], column, row);
            }
        }

        const optional<string> collection_name(this->controller.Ostats.name);
        auto write_intermediate = [&](
            const string& caption,
            const string& file_tag,
            const ComparisonTableAccumulator& table)
        {
            Persistence::write_comparison_table_csv(
                caption, file_tag, workload_names, table.averages(), false, {},
                collection_name);
        };
        write_intermediate(
            "Average throughput across all executed indices (q/s)",
            "average-all-indices", average_all_indices);
        write_intermediate(
            "Throughput of the winning index (best overall, q/s)",
            "winner", winner);
        write_intermediate(
            "Throughput of the best SynthDex index (q/s)",
            "best-synthdex", best_synthdex);
        write_intermediate(
            "Throughput of the best non-SynthDex manual index (q/s)",
            "best-manual", best_manual);
        write_intermediate(
            "Construction time of the best SynthDex index (seconds)",
            "best-synthdex-time", best_synthdex_time);
        write_intermediate(
            "Size of the best SynthDex index (megabytes)",
            "best-synthdex-size", best_synthdex_size);
        write_intermediate(
            "Construction time of the best manual index (seconds)",
            "best-manual-time", best_manual_time);
        write_intermediate(
            "Size of the best manual index (megabytes)",
            "best-manual-size", best_manual_size);

        const auto local_best_synthdex = best_synthdex.averages();
        const auto local_best_manual = best_manual.averages();
        const auto local_best_synthdex_time = best_synthdex_time.averages();
        const auto local_best_manual_time = best_manual_time.averages();
        const auto local_best_synthdex_size = best_synthdex_size.averages();
        const auto local_best_manual_size = best_manual_size.averages();
        ComparisonTable local_synthdex_advantage(
            workloads.size(), vector<double>(workloads.size(),
                numeric_limits<double>::quiet_NaN()));
        ComparisonTable local_synthdex_time_advantage(
            workloads.size(), vector<double>(workloads.size(),
                numeric_limits<double>::quiet_NaN()));
        ComparisonTable local_synthdex_size_advantage(
            workloads.size(), vector<double>(workloads.size(),
                numeric_limits<double>::quiet_NaN()));
        for (size_t row = 0; row < workloads.size(); ++row)
        {
            for (size_t column = 0; column < workloads.size(); ++column)
            {
                const double synthdex = local_best_synthdex[row][column];
                const double manual = local_best_manual[row][column];
                if (isfinite(synthdex) && isfinite(manual) && manual > 0.0)
                {
                    local_synthdex_advantage[row][column]
                        = max(0.0, (synthdex / manual - 1.0) * 100.0);

                    if (local_synthdex_advantage[row][column] == 0.0)
                    {
                        local_synthdex_time_advantage[row][column] = 0.0;
                        local_synthdex_size_advantage[row][column] = 0.0;
                    }
                }

                const double synthdex_time = local_best_synthdex_time[row][column];
                const double manual_time = local_best_manual_time[row][column];
                if (isfinite(synthdex_time) && isfinite(manual_time)
                    && isfinite(local_synthdex_advantage[row][column])
                    && local_synthdex_advantage[row][column] > 0.0
                    && synthdex_time > 0.0 && manual_time > 0.0)
                    local_synthdex_time_advantage[row][column]
                        = (manual_time / synthdex_time - 1.0) * 100.0;

                const double synthdex_size = local_best_synthdex_size[row][column];
                const double manual_size = local_best_manual_size[row][column];
                if (isfinite(synthdex_size) && isfinite(manual_size)
                    && isfinite(local_synthdex_advantage[row][column])
                    && local_synthdex_advantage[row][column] > 0.0
                    && synthdex_size > 0.0 && manual_size > 0.0)
                    local_synthdex_size_advantage[row][column]
                        = (manual_size / synthdex_size - 1.0) * 100.0;
            }
        }
        Persistence::write_comparison_table_csv(
            "SynthDex throughput advantage over the best manual index (percent)",
            "synthdex-advantage", workload_names,
            local_synthdex_advantage, true, {}, collection_name);
        Persistence::write_comparison_table_csv(
            "SynthDex construction-time advantage over the best manual index (percent)",
            "synthdex-advantage-time", workload_names,
            local_synthdex_time_advantage, true, {}, collection_name);
        Persistence::write_comparison_table_csv(
            "SynthDex size advantage over the best manual index (percent)",
            "synthdex-advantage-size", workload_names,
            local_synthdex_size_advantage, true, {}, collection_name);

        vector<vector<string>> ratio_display(
            workloads.size(), vector<string>(workloads.size()));
        for (size_t row = 0; row < workloads.size(); ++row)
            for (size_t column = 0; column < workloads.size(); ++column)
                if (row != column)
                    ratio_display[row][column]
                        = format_ratio(ratios[column] / 100.0) + ":"
                        + format_ratio(ratios[row] / 100.0);
        ComparisonTable ratio_values(
            workloads.size(), vector<double>(workloads.size(),
                numeric_limits<double>::quiet_NaN()));
        Persistence::write_comparison_table_csv(
            "Derived workload ratios (column workload : row workload)",
            "ratios", workload_names, ratio_values, false, ratio_display,
            collection_name);
    }

    this->controller.Q.clear();
    this->controller.Q_patterns.clear();

    ComparisonTableAccumulator average_all_indices(workloads.size());
    ComparisonTableAccumulator winner(workloads.size());
    ComparisonTableAccumulator best_synthdex(workloads.size());
    ComparisonTableAccumulator best_manual(workloads.size());
    ComparisonTableAccumulator best_synthdex_time(workloads.size());
    ComparisonTableAccumulator best_synthdex_size(workloads.size());
    ComparisonTableAccumulator best_manual_time(workloads.size());
    ComparisonTableAccumulator best_manual_size(workloads.size());
    vector<double> ratio_sums(workloads.size(), 0.0);
    size_t ratio_count = 0;
    optional<vector<string>> intermediate_workload_pattern;

    const string intermediate_dir = Cfg::get_out_dir()
        + "/comparison/intermediate/";
    if (filesystem::exists(intermediate_dir))
    {
        for (const auto& entry : filesystem::directory_iterator(intermediate_dir))
        {
            if (!entry.is_regular_file()) continue;
            const string tag = intermediate_tag(
                entry.path().filename().string());
            if (tag.empty()) continue;

            const string intermediate_path = entry.path().string();
            const auto intermediate = read_intermediate_table(
                intermediate_path, tag == "ratios");
            if (!intermediate_workload_pattern.has_value())
            {
                intermediate_workload_pattern = intermediate.workloads;
                if (intermediate.workloads != workload_names)
                    throw runtime_error(
                        "Comparison intermediate workload header mismatch in file: "
                        + intermediate_path);
            }
            else if (intermediate.workloads != *intermediate_workload_pattern)
            {
                throw runtime_error(
                    "Comparison intermediate workload header mismatch in file: "
                    + intermediate_path);
            }
            if (tag == "ratios")
            {
                for (size_t column = 0; column < workloads.size(); ++column)
                    ratio_sums[column] += intermediate.values[0][column];
                ratio_count++;
                continue;
            }

            ComparisonTableAccumulator* target = nullptr;
            if (tag == "average-all-indices") target = &average_all_indices;
            else if (tag == "winner") target = &winner;
            else if (tag == "best-synthdex") target = &best_synthdex;
            else if (tag == "best-manual") target = &best_manual;
            else if (tag == "best-synthdex-time") target = &best_synthdex_time;
            else if (tag == "best-synthdex-size") target = &best_synthdex_size;
            else if (tag == "best-manual-time") target = &best_manual_time;
            else if (tag == "best-manual-size") target = &best_manual_size;
            else continue;

            for (size_t row = 0; row < workloads.size(); ++row)
                for (size_t column = 0; column < workloads.size(); ++column)
                    if (isfinite(intermediate.values[row][column]))
                        target->add(row, column,
                            intermediate.values[row][column]);
        }
    }

    vector<vector<string>> ratio_table(
        workloads.size(), vector<string>(workloads.size()));
    if (ratio_count > 0)
    {
        vector<double> average_ratios(workloads.size());
        for (size_t workload_idx = 0; workload_idx < workloads.size(); ++workload_idx)
            average_ratios[workload_idx] = ratio_sums[workload_idx] / ratio_count;

        for (size_t row = 0; row < workloads.size(); ++row)
            for (size_t column = 0; column < workloads.size(); ++column)
                if (row != column)
                    ratio_table[row][column]
                        = format_ratio(average_ratios[column]) + ":"
                        + format_ratio(average_ratios[row]);
    }

    const auto best_synthdex_table = best_synthdex.averages();
    const auto best_manual_table = best_manual.averages();
    const auto best_synthdex_time_table = best_synthdex_time.averages();
    const auto best_synthdex_size_table = best_synthdex_size.averages();
    const auto best_manual_time_table = best_manual_time.averages();
    const auto best_manual_size_table = best_manual_size.averages();
    vector<vector<double>> synthdex_advantage(
        workloads.size(), vector<double>(workloads.size(),
            numeric_limits<double>::quiet_NaN()));
    vector<vector<double>> synthdex_time_advantage(
        workloads.size(), vector<double>(workloads.size(),
            numeric_limits<double>::quiet_NaN()));
    vector<vector<double>> synthdex_size_advantage(
        workloads.size(), vector<double>(workloads.size(),
            numeric_limits<double>::quiet_NaN()));
    for (size_t row = 0; row < workloads.size(); ++row)
    {
        for (size_t column = 0; column < workloads.size(); ++column)
        {
            const double synthdex = best_synthdex_table[row][column];
            const double manual = best_manual_table[row][column];
            if (isfinite(synthdex) && isfinite(manual) && manual > 0.0)
            {
                const double relative_advantage =
                    (synthdex / manual - 1.0) * 100.0;
                synthdex_advantage[row][column]
                    = max(0.0, relative_advantage);

                if (synthdex_advantage[row][column] == 0.0)
                {
                    synthdex_time_advantage[row][column] = 0.0;
                    synthdex_size_advantage[row][column] = 0.0;
                }
            }

            const double synthdex_time = best_synthdex_time_table[row][column];
            const double manual_time = best_manual_time_table[row][column];
            if (isfinite(synthdex_time) && isfinite(manual_time)
                && isfinite(synthdex_advantage[row][column])
                && synthdex_advantage[row][column] > 0.0
                && synthdex_time > 0.0 && manual_time > 0.0)
            {
                synthdex_time_advantage[row][column]
                    = (manual_time / synthdex_time - 1.0) * 100.0;
            }

            const double synthdex_size = best_synthdex_size_table[row][column];
            const double manual_size = best_manual_size_table[row][column];
            if (isfinite(synthdex_size) && isfinite(manual_size)
                && isfinite(synthdex_advantage[row][column])
                && synthdex_advantage[row][column] > 0.0
                && synthdex_size > 0.0 && manual_size > 0.0)
            {
                synthdex_size_advantage[row][column]
                    = (manual_size / synthdex_size - 1.0) * 100.0;
            }
        }
    }

    vector<vector<double>> ratio_values(
        workloads.size(), vector<double>(workloads.size(),
            numeric_limits<double>::quiet_NaN()));
    Persistence::write_comparison_table_csv(
        "Derived workload ratios (column workload : row workload)",
        "ratios", workload_names, ratio_values, false, ratio_table);
    Persistence::write_comparison_table_csv(
        "Average throughput across all executed indices (q/s)",
        "average-all-indices", workload_names,
        average_all_indices.averages());
    Persistence::write_comparison_table_csv(
        "Throughput of the winning index (best overall, q/s)",
        "winner", workload_names, winner.averages());
    Persistence::write_comparison_table_csv(
        "Throughput of the best SynthDex index (q/s)",
        "best-synthdex", workload_names, best_synthdex.averages());
    Persistence::write_comparison_table_csv(
        "Throughput of the best non-SynthDex manual index (q/s)",
        "best-manual", workload_names, best_manual.averages());
    Persistence::write_comparison_table_csv(
        "SynthDex throughput advantage over the best manual index (percent)",
        "synthdex-advantage", workload_names, synthdex_advantage, true);
    Persistence::write_comparison_table_csv(
        "Construction time of the best SynthDex index (seconds)",
        "best-synthdex-time", workload_names, best_synthdex_time.averages());
    Persistence::write_comparison_table_csv(
        "Size of the best SynthDex index (megabytes)",
        "best-synthdex-size", workload_names, best_synthdex_size.averages());
    Persistence::write_comparison_table_csv(
        "Construction time of the best manual index (seconds)",
        "best-manual-time", workload_names, best_manual_time.averages());
    Persistence::write_comparison_table_csv(
        "Size of the best manual index (megabytes)",
        "best-manual-size", workload_names, best_manual_size.averages());
    Persistence::write_comparison_table_csv(
        "SynthDex construction-time advantage over the best manual index (percent)",
        "synthdex-advantage-time", workload_names,
        synthdex_time_advantage, true);
    Persistence::write_comparison_table_csv(
        "SynthDex size advantage over the best manual index (percent)",
        "synthdex-advantage-size", workload_names,
        synthdex_size_advantage, true);
}
