#ifndef _COMPARISONRUNNER_H_
#define _COMPARISONRUNNER_H_

#include <string>
#include <vector>

using namespace std;

class Controller;

class ComparisonRunner
{
public:
    explicit ComparisonRunner(Controller &controller);

    void run();

private:
    static vector<string> split_pipe(const string &spec);
    static vector<string> expand_workloads(const string &spec);
    static string join_pipe(const vector<string> &parts);
    static vector<string> split_indices(const string &spec);

    Controller &controller;
};

#endif // _COMPARISONRUNNER_H_
