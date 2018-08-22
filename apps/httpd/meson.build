swagger_prog = find_program(top_dir + '/scripts/seastar-json2code.py')
swagger_cmd = [swagger_prog, '-f', '@INPUT@', '-o', '@OUTPUT@']
demo_hh = custom_target('demo_hh',
                        input : 'demo.json',
                        output : '@PLAINNAME@.hh',
                        command : swagger_cmd)

executable('httpd',
           [demo_hh, 'main.cc'],
           dependencies: seastar_dep,
           implicit_include_directories : false)
