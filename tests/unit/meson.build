# FIXME: The tls test is disabled. See github issue 514.
# FIXME: The slab test is disabled. See github issue 520.

seastar_tests = [
  'abort_source',
  'alloc',
  'allocator',
  'checked_ptr',
  'chunked_fifo',
  'circular_buffer',
  'circular_buffer_fixed_capacity',
  'connect',
  'defer',
  'deleter',
  'dns',
  'execution_stage',
  'expiring_fifo',
  'fair_queue',
  'file_io',
  'foreign_ptr',
  'futures',
  'httpd',
  'ipv6',
  'json_formatter',
  'lowres_clock',
  'metrics',
  'network_interface',
  'output_stream',
  'queue',
  'rpc',
  'scheduling_group',
  'semaphore',
  'sharded',
  'signal',
  'thread',
  'unix_domain',
]

#FIXME: check why they are exclusive
exclusive_tests = [
  'fstream',
  'stall_detector',
]

app_tests = [
  'alien',
  'directory',
  'distributed',
  'smp',
  'socket',
  'thread_context_switch',
  'timer',
]

other_tests = [
  'net_config',
  'noncopyable_function',
  'packet',
  'program_options',
  'shared_ptr',
  'simple_stream',
  'sstring',
  'tuple_utils',
  'uname',
  'unwind',
  'weak_ptr'
]

foreach t : seastar_tests + exclusive_tests + app_tests + other_tests
  n = t + '_test'
  e = executable(n,
                 n + '.cc',
                 include_directories: all_incdir,
                 dependencies : [seastar_dep, seastar_testing_dep])
  if t in seastar_tests + exclusive_tests
    args = ['--', '-c', '2']
  elif t in app_tests
    args = ['-c', '2']
  else
    args = []
  endif
  test(n, e, args: args, timeout: 300, is_parallel: t not in exclusive_tests)
endforeach
