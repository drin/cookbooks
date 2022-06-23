// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

shared_ptr<RecordBatch>
ConstructTestBatch(int64_t row_count, int col_count) {
    ArrayVector batch_data;
    batch_data.reserve(col_count);

    // >> First, define the schema
    // auto str_chunkedarr = std::make_shared<ChunkedArray>(str_arrvec, arrow::utf8());
    vector<shared_ptr<Field>> schema_fields;
    schema_fields.reserve(col_count);

    for (int col_ndx = 0; col_ndx < col_count; ++col_ndx) {
        string field_name { "col" + std::to_string(col_ndx) };
        schema_fields.push_back(arrow::field(field_name, arrow::uint32()));
    }

    // >> Second, construct the batch data itself
    // For each column from 0 to `col_count`
    for (int col_ndx = 0; col_ndx < col_count; ++col_ndx) {
        string val_prefix { "col" + std::to_string(col_ndx) };

        vector<string> col_vals;
        col_vals.reserve(row_count);

        // For each row from 0 to `row_count`
        for (int row_ndx = 0; row_ndx < row_count; ++row_ndx) {
          string val_suffix { ":val" + std::to_string(row_ndx) };
          string col_val    { val_prefix + val_suffix          };

          col_vals.push_back(col_val);
        }

        auto str_arr_result = ConstructStrArray(col_vals);
        batch_data.push_back(*str_arr_result);
    }

    auto batch_schema = arrow::schema(schema_fields);
    return RecordBatch::Make(batch_schema, row_count, batch_data);
}

/**
 * A simple main function that just constructs a single table containing a single column that is
 * backed by a `arrow::DictionaryArray`.
 */
int main(int argc, char **argv) {
    // Make some test data
    shared_ptr<RecordBatch> input_batch = ConstructTestBatch(5, 5);

    vector<int> col_indices = { 1, 3 };
    auto        col_bufsize = CalculateTempStackSize<StringType, StringArray>(
        std::static_pointer_cast<StringArray>(input_batch->column(0))
    );

    // Call a convenience wrapper around `arrow::compute::exec::Hashing32::HashBatch`
    auto hash_status = HashBatchColumns(
         input_batch
        ,col_indices
        ,col_bufsize * col_indices.size()
    );

    if (not hash_status.ok()) {
        std::cerr << "Error when hashing the data:" << std::endl
                  << "\t" << hash_status.message()  << std::endl
        ;

        return 1;
    }

    // View the result
    std::cout << "Hash status: " << hash_status.ToString() << std::endl;

    return 0;
}
