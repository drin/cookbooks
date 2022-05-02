
// ------------------------------
// Dependencies
#include "timing.hpp"


// ------------------------------
// Functions
std::string
TickToMS(clock_tick tick_val) {
    auto&& tick_tstamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        tick_val.time_since_epoch()
    );

    // NOTE: using string as return type is easier than
    //       figuring out what type `count()` returns
    return std::to_string(tick_tstamp.count());
}


std::string
CountTicks(clock_tick tick_start, clock_tick tick_stop) {
    auto&& tick_tstamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        tick_stop - tick_start
    );

    // NOTE: using string as return type is easier than
    //       figuring out what type `count()` returns
    return std::to_string(tick_tstamp.count());
}
