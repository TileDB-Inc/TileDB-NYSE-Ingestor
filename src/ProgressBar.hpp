#ifndef PROGRESSBAR_PROGRESSBAR_HPP
#define PROGRESSBAR_PROGRESSBAR_HPP

#include <chrono>
#include <iostream>
#include <utils.h>

class ProgressBar {
private:
    unsigned int ticks = 0;

    const unsigned int total_ticks;
    const unsigned int bar_width;
    const char complete_char = '=';
    const char incomplete_char = ' ';
    std::string label;
    const std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();

public:
    ProgressBar(std::string label, unsigned int total, unsigned int width, char complete, char incomplete) :
            label {label}, total_ticks {total}, bar_width {width},
            complete_char {complete}, incomplete_char {incomplete} {}

    ProgressBar(std::string label, unsigned int total, unsigned int width) :
            label {label}, total_ticks {total}, bar_width {width} {}

    unsigned int operator++() { return ++ticks; }

    void display() const
    {
        // Only update for each percentage

        if ( (ticks != total_ticks) && (ticks % (total_ticks/100+1) != 0) ) return;
        float progress = (float) ticks / total_ticks;
        int pos = (int) (bar_width * progress);

        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::seconds>(now-start_time).count();

        auto rate = static_cast<double>(ticks) / time_elapsed;
        std::cout << label <<  " [";

        for (int i = 0; i < bar_width; ++i) {
            if (i < pos) std::cout << complete_char;
            else if (i == pos) std::cout << ">";
            else std::cout << incomplete_char;
        }
        std::cout << "] " << int(progress * 100.0) << "% ("
                  << ticks << " / " << total_ticks << ") "
                  << rate << " rows/s, "
                  << nyse::beautify_duration(std::chrono::seconds((long)((total_ticks - ticks)  / rate)))  << " eta \r";
        std::cout.flush();
    }

    void done() const
    {
        display();
        std::cout << std::endl;
    }
};

#endif //PROGRESSBAR_PROGRESSBAR_HPP
