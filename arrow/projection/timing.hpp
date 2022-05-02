/**
 * Header for code that gets timestamps
 */


// ------------------------------
// Dependencies

#include <chrono>
#include <string>

// ------------------------------
// Macros and aliases

using clock_tick = std::chrono::time_point<std::chrono::steady_clock>;


// ------------------------------
// Functions
std::string TickToMS(clock_tick tick_val);
std::string CountTicks(clock_tick tick_start, clock_tick tick_stop);
