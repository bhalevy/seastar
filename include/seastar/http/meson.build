ragel_prog = find_program('ragel')
ragel_cmd = [ragel_prog, '-G2', '-o', '@OUTPUT@', '@INPUT@']

ragel_sed = ['sed', '-e', '1h;2,$H;$!d;g', '-re', 's/static const char _nfa[^;]*;//g', '@INPUT@']

parsers = []

foreach p : ['request', 'response']
    t = custom_target(p + '_parser_in',
                      input: top_dir + '/src/http/' + p + '_parser.rl',
                      output: p + '_parser.hh.in',
                      command: ragel_cmd)
    parsers += custom_target(p + '_parser',
                             input: t,
                             output: p + '_parser.hh',
                             capture: true,
                             command: ragel_sed)
endforeach
