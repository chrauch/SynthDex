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

#include "logging.h"
#include <fstream>
#include <iomanip>
#include <ctime>
#include <sstream>
#include <filesystem>
#include <string>
#include <omp.h>
#include "../utils/persistence.h"


int Log::level_output = 1;
int Log::caption_width = 30;

string Log::last_section = "";


void Log::w(
    const int &level,
    const string &caption,
    const string &msg)
{
    
    string padded_caption = caption;
    if (padded_caption.length() < Log::caption_width)
        padded_caption += string(Log::caption_width - padded_caption.length(), ' ');
    else
        padded_caption = padded_caption.substr(0, Log::caption_width);

    string msgx = msg;

    msgx.erase(msgx.find_last_not_of(" \n") + 1);

    size_t pos = 0;
    bool has_newline = false;
    while ((pos = msgx.find('\n', pos)) != string::npos)
    {
        msgx.replace(pos, 1, "\n" + string(Log::caption_width + 2, ' '));
        pos += 2;
        has_newline = true;
    }
    
    string text = padded_caption + ": " + msgx;

    Log::w(level, text);
}


void Log::w(const int &level, const string &msg)
{
    string msgx = indent(level, msg);
    
    // Clean
    if (!msgx.empty())
    {
        msgx.erase(msgx.find_last_not_of(" \n\t") + 1);
        msgx += '\n';
    }

    if (level <= 1) cout << msgx << flush;

    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm tm;
    localtime_r(&t, &tm);

    // Format date and time
    ostringstream date_s, time_s;
    date_s << put_time(&tm, "%Y%m%d-%H");
    time_s << put_time(&tm, "%Y-%m-%d [%H:%M]");

    #pragma omp critical
    {
        // Prepare file path
        string dir = Cfg::get<string>("out.dir") + "/logs";
        filesystem::create_directories(dir);
        string filename = dir + "/" + date_s.str() + ".log";

        // Write to file
        string section = "\n# " + time_s.str() + "\n";
        ofstream ofs(filename, ios::app);
        if (ofs) 
        {
            if (Log::last_section != section) ofs << section;
            ofs << msgx;
        }
        Log::last_section = section;
    }
}


string indent(int level, const string &text)
{
    istringstream iss(text);
    ostringstream oss;
    string line;
    string prefix(level, '\t');

    while (getline(iss, line))
        oss << prefix << line << '\n';

    return oss.str();
}


string progress(
    const string &caption, 
    const int &current,
    const int &total)
{
    return caption 
        + " (" + to_string(current + 1) + "/" + to_string(total) + ")";
}
