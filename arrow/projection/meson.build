# ------------------------------
# Project definition

project(
  'projection-recipe', 'cpp',
  version            : '0.1',
  default_options    : [ 'warning_level=3', 'cpp_std=c++17' ]
)


# ------------------------------
# Dependencies

dep_arrow = dependency('arrow-dataset', version: '>=12.0.1', static: false)


# ------------------------------
# Binaries to create

# recipe just shows basic usage
exe_recipe = executable('projection-recipe'
  ,'main.cpp'
  ,'recipe.cpp'
  ,'storage.cpp'
  ,dependencies : dep_arrow
  ,install      : false
)

# use projection on dataset for cluster 8 of E-GEOD-76312
exe_recipe = executable('projection-from-dataset'
  ,'project_from_dataset.cpp'
  ,'recipe.cpp'
  ,'storage.cpp'
  ,'timing.cpp'
  ,dependencies : dep_arrow
  ,install      : false
)


# ------------------------------
# Test targets

# Default test target, but I don't think we need these
# test('basic', exe)
