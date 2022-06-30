# ------------------------------
# Project definition

project(
  'add-compute-fn', 'cpp',
  version         : '0.1',
  default_options : [ 'warning_level=3', 'cpp_std=c++17' ]
)


# ------------------------------
# Dependencies

dep_arrow = dependency('arrow-dataset', version: '>=8.0.0', static: false)


# ------------------------------
# Binaries to create

simple_recipe = executable('simple'
  ,'simple-main.cc'
  ,'recipe.cc'
  ,dependencies : dep_arrow
  ,install      : false
)


# ------------------------------
# Test targets

# Default test target, but I don't think we need these
# test('basic', exe)
