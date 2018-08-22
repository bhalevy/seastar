public_incdir = include_directories(['../include'])
all_incdir = include_directories(['../include', '.'])

boost_dep = dependency('boost', modules : ['program_options', 'thread'])
gnutls_dep = dependency('gnutls')
libcares_dep = dependency('libcares')
lz4_dep = dependency('liblz4', version : '>=1.8.0')
yaml_dep = dependency('yaml-cpp')
cryptopp_dep = dependency('cryptopp')
protoc_prog = find_program('protoc')

compiler = meson.get_compiler('cpp')

dl_dep = compiler.find_library('dl')
fmt_dep = compiler.find_library('fmt')
rt_dep = compiler.find_library('rt')

dependencies = [
    boost_dep,
    cryptopp_dep,
    dl_dep,
    fmt_dep,
    gnutls_dep,
    libcares_dep,
    lz4_dep,
    rt_dep,
    yaml_dep,
]

subdir('proto')

srcs = [
  'core/alien.cc',
  'core/app-template.cc',
  'core/dpdk_rte.cc',
  'core/exception_hacks.cc',
  'core/execution_stage.cc',
  'core/fair_queue.cc',
  'core/file.cc',
  'core/fsqual.cc',
  'core/fstream.cc',
  'core/future-util.cc',
  'core/future.cc',
  'core/io_queue.cc',
  'core/linux-aio.cc',
  'core/memory.cc',
  'core/metrics.cc',
  'core/posix.cc',
  'core/prometheus.cc',
  'core/reactor.cc',
  'core/reactor_backend.cc',
  'core/resource.cc',
  'core/scollectd.cc',
  'core/sharded.cc',
  'core/smp.cc',
  'core/systemwide_memory_barrier.cc',
  'core/thread.cc',
  'core/thread_pool.cc',
  'core/uname.cc',
  'http/api_docs.cc',
  'http/common.cc',
  'http/file_handler.cc',
  'http/httpd.cc',
  'http/json_path.cc',
  'http/matcher.cc',
  'http/mime_types.cc',
  'http/reply.cc',
  'http/routes.cc',
  'http/transformers.cc',
  'json/formatter.cc',
  'json/json_elements.cc',
  'net/arp.cc',
  'net/config.cc',
  'net/dhcp.cc',
  'net/dns.cc',
  'net/dpdk.cc',
  'net/ethernet.cc',
  'net/inet_address.cc',
  'net/ip.cc',
  'net/ip_checksum.cc',
  'net/native-stack.cc',
  'net/net.cc',
  'net/packet.cc',
  'net/posix-stack.cc',
  'net/proxy.cc',
  'net/socket_address.cc',
  'net/stack.cc',
  'net/tcp.cc',
  'net/tls.cc',
  'net/udp.cc',
  'net/virtio.cc',
  'rpc/lz4_compressor.cc',
  'rpc/lz4_fragmented_compressor.cc',
  'rpc/rpc.cc',
  'util/alloc_failure_injector.cc',
  'util/backtrace.cc',
  'util/conversions.cc',
  'util/log.cc',
  'util/program-options.cc',
  'util/read_first_line.cc',
  metrics2_files
] + parsers

seastar = static_library('seastar',
                         srcs,
                         dependencies: dependencies,
                         include_directories: all_incdir)
seastar_dep = declare_dependency(link_with: seastar,
                                 include_directories: public_incdir,
                                 sources: parsers + [metrics2_files[0]])
subdir('testing')
