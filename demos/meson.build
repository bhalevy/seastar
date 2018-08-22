demos = [
  'block_discard',
  'echo',
  'ip',
  'l3',
  'line_count',
  'rpc',
  'scheduling_group',
  'tcp',
  'tcp_sctp_client',
  'tcp_sctp_server',
  'tls_echo_server',
  'tls_simple_client',
  'udp_client',
  'udp_server',
  'udp_zero_copy',
]

foreach d : demos
  executable(d + '_demo',
             d + '_demo.cc',
             dependencies : seastar_dep,
             implicit_include_directories : false)
endforeach
