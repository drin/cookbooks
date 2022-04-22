# ------------------------------
# Project definition

project(
  'dictarray-recipe', 'cpp',
  version         : '0.1',
  default_options : [ 'warning_level=3', 'cpp_std=c++17' ]
)


# ------------------------------
# Dependencies

dep_arrow = dependency('arrow-dataset', version: '>=7.0.0', static: false)


# ------------------------------
# Binaries to create

# recipe just shows basic usage
exe_recipe = executable('dictarray-recipe'
  ,'main.cpp'
  ,'storage.cpp'
  ,'recipe.cpp'
  ,dependencies : dep_arrow
  ,install      : false
)

# writer allows us to profile the use of a DictionaryArray in a Table
exe_writer = executable('write-test'
  ,'writer.cpp'
  ,'storage.cpp'
  ,'recipe.cpp'
  ,dependencies : dep_arrow
  ,install      : false
)

# reader allows us to profile the use of a DictionaryArray in a Table
exe_reader = executable('read-test'
  ,'reader.cpp'
  ,'storage.cpp'
  ,'recipe.cpp'
  ,dependencies : dep_arrow
  ,install      : false
)


# ------------------------------
# Test targets

# Default test target, but I don't think we need these
# test('basic', exe)
