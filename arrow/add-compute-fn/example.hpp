#pragma once


// ------------------------------
// Dependencies

// consolidated dependencies to keep this header concise
#include "support.hpp"


// ------------------------------
// Functions

// >> Function declaration

/*
 * Declaration for a function that serves as an "ergonomic" interface to a function in the
 * function registry.
 */
ARROW_EXPORT
Result<Datum> AbsoluteValue( const Datum             &arg
                            ,      ArithmeticOptions  options = ArithmeticOptions()
                            ,      ExecContext       *ctx     = NULLPTR);
