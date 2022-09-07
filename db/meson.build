ycsbc_db_source = []
ycsbc_db_source += files(
    'db_factory.cc',
    'rocksdb.cc'
)


ycsbc_db_lib = library(
    'ycsbc-db',
    ycsbc_db_source,
    dependencies: project_dependencies,
    include_directories: project_include_directories,
    link_args: project_link_flags,
    build_rpath: rocksdb_lib_dir,
    install_rpath: rocksdb_lib_dir,
)

project_source_files += ycsbc_db_source
project_header_files += files(
    'basic_db.h',
    'db_factory.h',
    'rocksdb.h',
)