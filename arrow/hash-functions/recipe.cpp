// ------------------------------
// Dependencies

// Local and third-party dependencies
#include "recipe.hpp"

// ------------------------------
// Macros and aliases

/*
constexpr uint32_t MAX_BATCHSIZE = MiniBatch::kMiniBatchLength;
constexpr uint32_t UNIT_SIZE     = 128;
*/

// ------------------------------
// Functions


/**
 * A recipe for calling `Hashing32::HashBatch`.
 *
 * This shows how to call HashBatch, which requires access to an `ExecContext` and also a
 * `TempVectorStack`. The TempVectorStack must be initialized before HashBatch can
 * allocate memory through it; additionally, the initialized size must be large enough to
 * accommodate memory allocated from it.
 */
Status
HashBatchColumns( shared_ptr<RecordBatch>  source_batch
                 ,vector<int>             &col_indices
                 ,int64_t                  expected_size) {
    ARROW_ASSIGN_OR_RAISE(auto process_batch, source_batch->SelectColumns(col_indices));

    auto             exec_ctx    = default_exec_context();
    auto             input_batch = ExecBatch(*process_batch);
    vector<uint32_t> result_hashes(input_batch.length);

    TempVectorStack  tmp_stack;
    auto init_status = tmp_stack.Init(
         exec_ctx->memory_pool()
        ,expected_size * process_batch->num_columns()
    );
    ARROW_WARN_NOT_OK(init_status, "Initialized TempVectorStack");

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

    ARROW_WARN_NOT_OK(arr_builder.Resize(src_vector.size()), "Resize");
    ARROW_WARN_NOT_OK(arr_builder.AppendValues(src_vector) , "Append");

    arrow::Status build_status = arr_builder.Finish(&str_array);
    if (not build_status.ok()) {
        std::cerr << "Failed to construct string array" << std::endl;
        return build_status;
    }

    return str_array;
}
