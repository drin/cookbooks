// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases


// ------------------------------
// Functions

/**
 * A recipe for calling `Index`. See:
 * https://github.com/apache/arrow/blob/apache-arrow-8.0.0/cpp/src/arrow/compute/kernels/aggregate_test.cc#L2234
 */
Status
HashBatchColumns(shared_ptr<RecordBatch> source_batch, vector<int> &col_indices) {
    ARROW_ASSIGN_OR_RAISE(auto process_batch, source_batch->SelectColumns(col_indices));

    auto             exec_ctx    = default_exec_context();
    auto             input_batch = ExecBatch(*process_batch);
    vector<uint32_t> result_hashes(input_batch.length);
    TempVectorStack  tmp_stack;

    auto hash_status = Hashing32::HashBatch(
         input_batch
        ,result_hashes.data()
        ,exec_ctx->cpu_info()->hardware_flags()
        ,&tmp_stack
        ,0
        ,input_batch.length
    );

    std::cout << "Result Hashes:" << std::endl
              << "\t" << std::to_string(result_hashes[0]) << std::endl
              << "\t" << std::to_string(result_hashes[1]) << std::endl
              << "\t" << std::to_string(result_hashes[2]) << std::endl
              << "\t" << std::to_string(result_hashes[3]) << std::endl
              << "\t" << std::to_string(result_hashes[4]) << std::endl
    ;

    return hash_status;
}


// ------------------------------
// Convenience Functions

// >> construction

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
