#include "recipe.hpp"

Result<shared_ptr<Array>>
BuildIntArray() {
  vector<int32_t> col_vals { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

  Int32Builder builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(col_vals.size()));
  ARROW_RETURN_NOT_OK(builder.AppendValues(col_vals));
  return builder.Finish();
}

int main(int argc, char **argv) {
  // >> Construct some test data
  auto build_result = BuildIntArray();
  if (not build_result.ok()) {
    std::cout << build_result.status().message() << std::endl;
    return 1;
  }

  // >> Peek at the data
  auto col_vals = *build_result;
  std::cout << col_vals->ToString() << std::endl;

  // >> Invoke compute function
  //  |> First, register
  auto fn_registry = arrow::compute::GetFunctionRegistry();
  RegisterNamedScalarFn(fn_registry);


  //  |> Then, invoke
  Datum col_as_datum { col_vals };
  auto fn_result = NamedScalarFn(col_as_datum);
  if (not fn_result.ok()) {
    std::cout << fn_result.status().message() << std::endl;
    return 2;
  }

  std::cout << fn_result->ToString() << std::endl;
  return 0;
}
