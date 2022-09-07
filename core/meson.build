ycsbc_core_source = []
ycsbc_core_source += files(
    'core_workload.cc'
)


ycsbc_core_lib = library(
    'ycsbc-core',
    ycsbc_core_source,
    dependencies: pthread_dep,
    include_directories: project_include_directories,
    link_args: project_link_flags,
)

project_source_files += ycsbc_core_source
project_header_files += files(
    'client.h',
    'const_generator.h',
    'core_workload.h',
    'counter_generator.h',
    'db.h',
    'discrete_generator.h',
    'generator.h',
    'properties.h',
    'scrambled_zipfian_generator.h',
    'skewed_latest_generator.h',
    'timer.h',
    'uniform_generator.h',
    'utils.h',
    'zipfian_generator.h',
)