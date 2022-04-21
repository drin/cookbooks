// ------------------------------
// Dependencies

// Main dependencies
#include <iostream>

// Local and third-party dependencies
#include "datatypes.hpp"

// ------------------------------
// Macros and aliases

#define PROJECT_NAME "ipc-dict-array"


// ------------------------------
// Functions

int main(int argc, char **argv) {
    if (argc != 1) {
        std::cout << argv[0] <<  "takes no arguments.\n";
        return 1;
    }

    std::cout << "This is project " << PROJECT_NAME << ".\n";

    return 0;
}
