memcached_ascii_in = custom_target('memcached_ascii_in',
                                   input: 'ascii.rl',
                                   output: 'ascii.hh.in',
                                   command: ragel_cmd)

memcached_ascii =  custom_target('memcached_ascii',
                                 input: memcached_ascii_in,
                                 output: 'ascii.hh',
                                 capture: true,
                                 command: ragel_sed)

memcached_incdir = include_directories('.')

executable('memcached',
           [memcached_ascii, 'memcache.cc'],
           dependencies: seastar_dep,
           include_directories: memcached_incdir,
           implicit_include_directories : false)

subdir('tests')
