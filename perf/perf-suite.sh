#!/bin/bash

if [ $# -ne 4 ];
then
  echo "USAGE: $0 data_dir report_dir max_size memory"
  exit 1
fi

# Some constants
DATA_DIR=${1}
REPORT_DIR=${2}
MAX_SIZE=${3}
MEMORY=${4}
BASE_DIR=$(dirname $0)
THROUGHPUT=10000
THREADS=8
VALUE_SIZE=1000
NUM_KEYS=$((MAX_SIZE/VALUE_SIZE))
INDEX_CAPACITY=$((2*NUM_KEYS))
OPS=$((10*MAX_SIZE/VALUE_SIZE))
export JVM_OPTS="-Xmx512M -Xms512M -d64 -XX:MaxDirectMemorySize=$MEMORY"
PERF_CMD="${BASE_DIR}/../run-class.sh valencia.PerformanceTest --directory $DATA_DIR"

mkdir -p $DATA_DIR $REPORT_DIR

# record test parameters 
echo "
DATA_DIR=$DATA_DIR
REPORT_DIR=$REPORT_DIR
MAX_SIZE=$MAX_SIZE
JVM_MEMORY=$JVM_MEMORY
BASE_DIR=$BASE_DIR
PERF_CMD=$PERF_CMD
THROUGHPUT=$THROUGHPUT
THREADS=$THREADS
VALUE_SIZE=$VALUE_SIZE
NUM_KEYS=$NUM_KEYS
INDEX_CAPACITY=$INDEX_CAPACITY
OPS=$OPS
" > $REPORT_DIR/params.txt

$PERF_CMD --no-test > $REPORT_DIR/header.txt

echo "Testing effect of data sizes..."
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_size_fixed_throughput.csv
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_size_max_throughput.csv
for i in {1..5};
do
  size_inc=$((MAX_SIZE/5))
  size=$((i*size_inc))
  num_keys=$((size/VALUE_SIZE))
  index_capacity=$((2*num_keys))
  BASE_CMD="$PERF_CMD --no-header --prepopulate --value-size $VALUE_SIZE --num-keys=$num_keys --cleanup --threads $THREADS --index-capacity $index_capacity"
  echo "Testing size=$size at throughput=$THROUGHPUT"
  $BASE_CMD --throughput $THROUGHPUT --ops $((THROUGHPUT*300)) >> $REPORT_DIR/vary_size_fixed_throughput.csv
  echo "Testing size=$size at max throughput"
  $BASE_CMD  --ops $num_keys >> $REPORT_DIR/vary_size_max_throughput.csv
done

echo "Testing effect of throughput..."
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_throughput.csv
for i in {1..5};
do
  throughput=$((i*10000))
  echo "Testing throughput=$throughput"
  $PERF_CMD --no-header --prepopulate --value-size $VALUE_SIZE --num-keys=$NUM_KEYS --cleanup --ops $((throughput*100)) --threads $THREADS --index-capacity $INDEX_CAPACITY --throughput $throughput >> $REPORT_DIR/vary_throughput.csv
done

echo "Testing effect of utilization..."
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_utilization_fixed_throughput.csv
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_utilization_max_throughput.csv
for i in {1..5};
do
  utilization=$((i*20))
  BASE_CMD="$PERF_CMD --no-header --prepopulate --value-size $VALUE_SIZE --num-keys=$NUM_KEYS --cleanup --threads $THREADS --index-capacity $INDEX_CAPACITY"
  echo "Testing utilization=$utilization at throughput=$THROUGHPUT"
  $BASE_CMD --ops $((300*THROUGHPUT)) --throughput $THROUGHPUT >> $REPORT_DIR/vary_utilization_fixed_throughput.csv
  echo "Testing utilization=$utilization at max throughput"
  $BASE_CMD --ops $OPS >> $REPORT_DIR/vary_utilization_max_throughput.csv
done

echo "Testing effect of value size..."
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_value_size_fixed_throughput.csv
cp $REPORT_DIR/header.txt $REPORT_DIR/vary_value_size_max_throughput.csv
for i in {1..5};
do
  value_size=$((8**i))
  num_keys=$((size/VALUE_SIZE))
  ops=$((100*num_keys))
  BASE_CMD="$PERF_CMD --no-header --prepopulate --value-size $VALUE_SIZE --num-keys=$num_keys --cleanup --ops $ops --threads $THREADS --index-capacity $INDEX_CAPACITY"
  echo "Testing value_size=$value_size at throughput=$THROUGHPUT"
  $BASE_CMD --throughput $THROUGHPUT >> $REPORT_DIR/vary_value_size_fixed_throughput.csv
  echo "Testing value_size=$value_size at max throughput"
  $BASE_CMD >> $REPORT_DIR/vary_value_size_max_throughput.csv
done

echo "Testing scan"
cp $REPORT_DIR/header.txt $REPORT_DIR/scan_impact.csv
for scans in {0..5};
do
  echo "Testing $scans background scans"
  $PERF_CMD --no-header --prepopulate --value-size $VALUE_SIZE --scan-threads $scans --num-keys=$NUM_KEYS --cleanup --ops $OPS --threads $THREADS --index-capacity $INDEX_CAPACITY --throughput $THROUGHPUT >> $REPORT_DIR/scan_impact.csv
done;

echo "Testing simple mixed performance"
$PERF_CMD --prepopulate --reporting-interval 30 --value-size $VALUE_SIZE --num-keys=$NUM_KEYS --cleanup --ops $OPS --threads $THREADS --index-capacity $INDEX_CAPACITY  > $REPORT_DIR/simple.csv

echo "Testing max write"
$PERF_CMD --prepopulate --reporting-interval 5 --percent-writes 100 --percent-reads 0 --percent-deletes 0 --value-size $VALUE_SIZE --num-keys=$NUM_KEYS --cleanup --ops $((2*OPS)) --threads $THREADS --index-capacity $INDEX_CAPACITY > $REPORT_DIR/max_write.csv

