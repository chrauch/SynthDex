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

#include "processexec.h"
#include <array>
#include <memory>
#include <cstdio>
#include <stdexcept>


string ProcessExec::run(const string &command, const int &log_level, const bool &interactive)
{
    // For interactive commands, use system() to allow terminal access
    if (interactive)
    {
        system(command.c_str());
        return "";
    }

    // For non-interactive commands, use popen() to capture output
    array<char, 128> buffer;
    string result;
    string line_buffer;
    int line_counter = 1;
    auto start_time = chrono::steady_clock::now();
    unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.c_str(), "r"), pclose);

    if (!pipe) throw runtime_error("popen failed");

    string old_prefix = "";
    auto get_elapsed_prefix = [&]()
    {
        auto elapsed = chrono::duration_cast<chrono::milliseconds>(
            chrono::steady_clock::now() - start_time).count();
        auto prefix = "... " + to_string(elapsed) + " ms";
        return prefix != old_prefix ? (old_prefix = prefix, prefix) : "";
    };

    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    {
        line_buffer += buffer.data();
        size_t pos;
        while ((pos = line_buffer.find('\n')) != string::npos)
        {
            string line = line_buffer.substr(0, pos);
            ProcessExec::log(log_level, get_elapsed_prefix(), line.c_str());
            line_buffer.erase(0, pos + 1);
            line_counter++;
        }
        result += buffer.data();
    }

    if (!line_buffer.empty())
        ProcessExec::log(log_level, get_elapsed_prefix(), line_buffer.c_str());

    return result;
}


void ProcessExec::log(int log_level, const string &caption, const string &msg)
{
    int tab_count = 0;
    for (char c : msg)
    {
        if (c == '\t') tab_count++;
        else break;
    }
    string new_msg = msg.substr(tab_count);

    // If message contains '=', split into caption and message
    size_t eq_pos = new_msg.find('=');
    if (eq_pos != string::npos && eq_pos < Log::caption_width) 
    {
        string extracted_caption = new_msg.substr(0, eq_pos);
        string extracted_msg = new_msg.substr(eq_pos + 1);

        // Trim leading/trailing whitespace
        auto ltrim = [](string &s)
        {
            s.erase(s.begin(), find_if(s.begin(), s.end(), [](int ch) {
                return !isspace(ch);
            }));
        };
        auto rtrim = [](string &s)
        {
            s.erase(find_if(s.rbegin(), s.rend(), [](int ch) {
                return !isspace(ch);
            }).base(), s.end());
        };
        ltrim(extracted_caption);
        rtrim(extracted_caption);
        ltrim(extracted_msg);
        rtrim(extracted_msg);
        Log::w(log_level + tab_count, extracted_caption, extracted_msg);
        return;
    }

    Log::w(log_level + tab_count, caption, new_msg);
}


string ProcessExec::python_run(const string &desc, const string &command,
    const int &log_level)
{
    auto python_exec = Cfg::get<string>("setup.interpreter");
    string full_command = python_exec + " " + command;
    Log::w(1, desc, full_command);
    return ProcessExec::run(full_command, log_level);
}


void ProcessExec::python_setup(bool force)
{
    if (!force && ProcessExec::python_venv_exists()) return;

    Log::w(0, "Environment");

    ProcessExec::run(Cfg::get<string>("setup.create"), 1);

    ProcessExec::python_run(
        "PIP update",
        Cfg::get<string>("setup.upgrade"), 1);

    auto libraries = Cfg::get<vector<string>>("setup.libraries");

    Log::w(0, "Libraries", strs(libraries, "\n"));

    for (const auto &l : libraries)
        ProcessExec::python_run(
            "PIP install",
            Cfg::get<string>("setup.install") + " " + l, 1);
}


bool ProcessExec::python_venv_exists()
{
    Log::w(2, "Python virtual environment check");

    auto python_exec = Cfg::get<string>("setup.interpreter");

    string check_command = python_exec + " --version 2>&1";
    try
    {
        auto output = ProcessExec::run(check_command, 3);
        Log::w(3, "Interpreter", python_exec);
        Log::w(3, "Version", output);
        return true;
    }
    catch (const exception &e)
    {
        Log::w(0, "Python virtual environment not found or not working.");
        return false;
    }
}