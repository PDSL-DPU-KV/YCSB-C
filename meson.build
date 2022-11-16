project(
    'ycsbc-rocksdb',
    'cpp',
    version:'1.0',
    default_options: ['cpp_std=c++11'],
)

rocksdb_lib_dir = '/home/dc/workspace/rocksdb-rc/build'
rocksdb_include_dir = '/home/dc/workspace/rocksdb-rc/include'

pthread_dep = dependency('threads')
protobuf_dep = dependency('protobuf', version: '>=3.20.1')
#protobuf_dep = dependency('protobuf')
# librocksdb_dep = dependency('librocksdb')
librocksdb_dep = declare_dependency(
    link_args: ['-L' + rocksdb_lib_dir, '-lrocksdb'],
    include_directories: include_directories(rocksdb_include_dir)
)

rapidjson = subproject('rapidjson')
rapidjson_dep = rapidjson.get_variable('rapidjson_dep')


project_dependencies = [
    pthread_dep,
    protobuf_dep,
    librocksdb_dep,
    rapidjson_dep
]

project_link_flags = []

project_header_files = []
project_source_files = []
project_include_directories = [
    include_directories('core'),
    include_directories('lib'),
    include_directories('db'),
]

subdir('core')
subdir('db')
subdir('lib')

executable(
    'ycsbc',
    'ycsbc.cc',
    include_directories: project_include_directories,
    dependencies: project_dependencies,
    link_with: [ycsbc_core_lib, ycsbc_db_lib],
    build_rpath: rocksdb_lib_dir,
    install_rpath: rocksdb_lib_dir,
)
