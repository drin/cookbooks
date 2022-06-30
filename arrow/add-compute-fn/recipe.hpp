#pragma once


// ------------------------------
// Dependencies

#include "support.hpp"


// ------------------------------
// Functions

// >> Function registration and kernel association
/** A convenience function that registers our new, named function. */
ARROW_EXPORT
void
RegisterNamedScalarFn(FunctionRegistry *registry);


// >> Convenience functions
/** A named function (visible in a "translation unit") that we will register. */
ARROW_EXPORT
Result<Datum>
NamedScalarFn(const Datum &input_arg, ExecContext *ctx = NULLPTR);
