// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

/**
 * How to construct a `arrow::StringArray` from a `std::vector<std::string>`.
 */
Result<shared_ptr<StringArray>>
ConstructStrArray(vector<string> src_vector) {
    shared_ptr<StringArray> str_array;
    arrow::StringBuilder    arr_builder;

    arr_builder.Resize(src_vector.size());
    arr_builder.AppendValues(src_vector);

    arrow::Status build_status = arr_builder.Finish(&str_array);
    if (not build_status.ok()) {
        std::cerr << "Failed to construct string array" << std::endl;
        return build_status;
    }

    return str_array;
}


/**
 * How to use `arrow::compute::DictionaryEncode` to create a `arrow::DictionaryArray`
 * from a `arrow::StringArray`.
 */
Result<shared_ptr<DictionaryArray>>
DictArrFromVal(vector<string> arr_vals) {
    ARROW_ASSIGN_OR_RAISE(auto str_array      , ConstructStrArray(arr_vals));
    ARROW_ASSIGN_OR_RAISE(auto wrapped_dictarr, DictionaryEncode(str_array));

    return std::static_pointer_cast<DictionaryArray>(
        std::move(wrapped_dictarr).make_array()
    );
}


/**
 * How to construct a `arrow::Table` containing a single column backed by a
 * `arrow::DictionaryArray`.
 */
Result<shared_ptr<Table>>
ConstructTestTable() {
    // ----------
    // >> Hard-coded test data
    string         test_colname { "test_col" };
    vector<string> testcol_vals {
         "first", "second", "third" , "fourth", "fifth"
        ,"first", "third" , "second", "fifth" , "fourth"
    };

    // ----------
    // >> Compose the test table structure

    //  |> first, create the column (as a DictionaryArray)
    ARROW_ASSIGN_OR_RAISE(auto test_colarray, DictArrFromVal(testcol_vals));

    //  |> then, for readability, create the schema
    auto test_coltype   = arrow::dictionary(arrow::int32(), arrow::utf8());
    auto test_tblschema = arrow::schema({ arrow::field(test_colname, test_coltype) });

    //  |> finally, create the table
    auto test_table  = Table::Make(test_tblschema, { test_colarray }, test_colarray->length());

    // [DEBUG] print the values for visibility
    std::cout << "Column length: " << test_colarray->length() << std::endl;
    std::cout << test_colarray->ToString()                    << std::endl;
    std::cout << test_table->ToString()                       << std::endl;

    return test_table;
}


// ------------------------------
// Verbose functions to clarify the API

/**
 * How to use `arrow::compute::DictionaryEncode` to create a `arrow::DictionaryArray`
 * from a `arrow::StringArray`.
 */
Result<shared_ptr<DictionaryArray>>
VerboseDictArrFromVal(vector<string> arr_vals) {
    // convert the string vector to a `arrow::StringArray`
    ARROW_ASSIGN_OR_RAISE(
         shared_ptr<StringArray> str_array
        ,ConstructStrArray(arr_vals)
    );

    // construct a `arrow::DictionaryArray` from the `arrow::StringArray`
    // NOTE: functions in the compute API take in `arrow::Datum`s and return
    //       `arrow::Datum`, but usually the inputs are implicitly converted.
    ARROW_ASSIGN_OR_RAISE(
         arrow::Datum wrapped_dictarr
        ,DictionaryEncode(str_array)
    );

    // The result cannot be implicitly converted, so we first use
    // `arrow::Datum::make_array` to extract the `arrow::Array`
    shared_ptr<Array> encoded_array = std::move(wrapped_dictarr).make_array();

    // Then we cast the shared_ptr from `arrow::Array` to `arrow::DictionaryArray`.
    // NOTE: arrow is somehow able to implicitly wrap the return in a `arrow::Result`
    //       if it is of the right type. Must be macro magic.
    return std::static_pointer_cast<DictionaryArray>(encoded_array);
}
