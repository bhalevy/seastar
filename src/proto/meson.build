metrics2_cmd = [
    protoc_prog,
    '--cpp_out=@OUTDIR@',
    '-I@CURRENT_SOURCE_DIR@',
    '@INPUT@'
]
metrics2_files = custom_target('metrics2_files',
                               input: 'metrics2.proto',
                               output: ['@BASENAME@.pb.h', '@BASENAME@.pb.cc'],
                               command: metrics2_cmd)
