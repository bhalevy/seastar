seastar_perf_testing = static_library ('seastar_perf_testing',
                                       'perf_tests.cc',
                                       dependencies : seastar_dep)

foreach p : ['future_util', 'fstream', 'rpc']
    executable(p + '_perf',
               p + '_perf.cc',
               dependencies : seastar_dep,
               link_with: seastar_perf_testing)
endforeach
