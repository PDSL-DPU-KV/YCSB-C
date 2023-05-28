## workload parameters
workload="drop"
workload_array=("drop" "load" "workloada")
threads=1

## db parameters
db=rocksdb
db_array=("rocksdb" "wiredtiger")
num_multi_db=1
#dbpath="/mnt/pmem0/test"
dbpath="/mnt/ssd/test"
dboptions="./config/rocksdb.json"
dbstatistics=1
dbwaitforbalance=1
perflevel=3

ycsb_path="$(dirname $PWD)/build/ycsbc"
bench_dir="$(dirname $PWD)/test"

tdate=$(date "+%Y_%m_%d_%H_%M_%S")

const_params=""

function fill_params() {
    if [ -n "$db" ];then
        const_params=$const_params"-db $db "
    fi

    if [ -n "$num_multi_db" ];then
        const_params=$const_params"-num_multi_db $num_multi_db "
    fi

    if [ -n "$dbpath" ];then
        const_params=$const_params"-dbpath $dbpath "
    fi

    if [ -n "$dbstatistics" ];then
        const_params=$const_params"-dbstatistics $dbstatistics "
    fi

    if [ -n "$dbwaitforbalance" ];then
        const_params=$const_params"-dbwaitforbalance $dbwaitforbalance "
    fi

    if [ -n "$perflevel" ];then
        const_params=$const_params"-perflevel $perflevel "
    fi

    if [ -n "$dboptions" ];then
        const_params=$const_params"-dboptions $dboptions "
    fi

    if [ -n "$workload" ];then
        const_params=$const_params"-P $workload "
    fi

    if [ -n "$threads" ];then
        const_params=$const_params"-threads $threads "
    fi
}

function clean_db() {
    if [ -n "$dbpath" ];then
        sudo rm -rf $dbpath/*
    fi
    sleep 2
}

function clean_cache() {
    sync
    echo 3 | sudo tee -a /proc/sys/vm/drop_caches > /dev/null
    sleep 2
}

# $1: workload
# $2: db option
function copy_out_file() {
    mkdir $bench_dir/result_$tdate > /dev/null 2>&1
    res_dir=$bench_dir/result_$tdate/$1\_$2
    mkdir $res_dir > /dev/null 2>&1
    \mv -f $bench_dir/out.out $res_dir/
    \mv -f $bench_dir/compaction.csv $res_dir/
    \mv -f $bench_dir/flush.csv $res_dir/
    \mv -f $bench_dir/file_access.csv $res_dir/
}

# $1: load/run
# $2: numa
function run_one_test() {
    const_params=""
    fill_params
    if [ $1 == "load" ]; then
        const_params=$const_params"-load 1"
    elif [ $1 == "run" ]; then
        const_params=$const_params"-run 1"
    fi

    cmd="sudo $ycsb_path $const_params | tee -a out.out"
    if [ $2 == "numa" ]; then 
        cmd="sudo numactl -N 1 $ycsb_path $const_params | tee -a out.out"
    fi
    echo $cmd >out.out
    echo $cmd
    eval $cmd
}

# $1: workload dir
# $2: dbs option
function run_all_test() {
    for wl in ${workload_array[@]}; do
        clean_cache
        if [ $wl == "drop" ]; then
            clean_db
            continue
        else 
            workload=$1/$wl.spec
            dboptions=$2
            if [ $wl == "load" ]; then
                workload=$1/workloada.spec
                run_one_test load numa
            else
                run_one_test run numa
            fi
        fi

        if [ $? -ne 0 ]; then
            exit 1
        fi
        copy_out_file $wl ${dboptions##*/}
        sleep 5
    done
}

run_all_test $1 $2
