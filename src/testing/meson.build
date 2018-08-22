test_incdir = include_directories(['.'])

boost_test_dep = dependency('boost', modules : ['unit_test_framework'])

seastar_testing = static_library('seastar_testing',
                                 ['entry_point.cc', 'seastar_test.cc', 'test_runner.cc'],
                                 include_directories: test_incdir,
                                 cpp_args: ['-DBOOST_TEST_DYN_LINK'],
                                 dependencies: [seastar_dep, boost_test_dep])
seastar_testing_dep = declare_dependency(link_with: seastar_testing,
                                         compile_args: ['-DSEASTAR_TESTING_MAIN'],
                                         include_directories: test_incdir)
