project(
  'ipc-dict-array', 'cpp',
  version         : '0.1',
  default_options : [ 'warning_level=3', 'cpp_std=c++17' ]
)

# Dependencies
dep_arrow = dependency('arrow-dataset', version: '>=7.0.0', static: false)

# arbitrary name for the executable we'll build
main_exe = executable('ipc_dict_array'
  ,'ipc_dict_array.cpp'
  ,dependencies : dep_arrow
  ,install      : false
)

# I don't think we need "test"
# test('basic', exe)
