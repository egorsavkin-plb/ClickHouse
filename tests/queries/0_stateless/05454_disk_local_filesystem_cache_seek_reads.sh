#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

CACHE_NAME='local_disk_cached'
READ_CACHED="enable_filesystem_cache = 1"
READ_UNCACHED="enable_filesystem_cache = 0"

# Regression coverage for checksum-mismatch errors (`CachedOnDiskReadBufferFromFile`)
# seen when a local disk uses the filesystem cache under concurrent, overlapping
# range reads of a big part: subset (granule-seek) reads populate segments
# partially, readers are dropped mid-segment (handing downloads to other readers
# or to background download), merges run in parallel, and every byte served must
# still match the source part files, which `CHECK TABLE` verifies end to end.

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_seek SYNC"
drop_filesystem_cache $CACHE_NAME

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_seek (x UInt64, a Array(UInt64) CODEC(NONE), b Array(UInt64) CODEC(NONE))
    ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 'local_disk_cached'"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_seek"
# One wide part; each array column file is ~100 MB, i.e. several cache segments.
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_seek
    SELECT number,
           arrayMap(i -> (i + number) % 1000, range(200)),
           arrayMap(i -> (i * 3 + number) % 997, range(200))
    FROM numbers(120000)"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_seek"

function agg_query()
{
    echo "SELECT count(), sum(arraySum(a)), sum(arraySum(b)) FROM t_seek SETTINGS $READ_CACHED"
}

echo 'Expected result without cache'
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(arraySum(a)), sum(arraySum(b)) FROM t_seek SETTINGS $READ_UNCACHED"

echo 'Subset reads prime only some granules and segments'
for range in 'x < 10000' 'x >= 30000 AND x < 45000' 'x >= 90000'; do
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(arraySum(a)) FROM t_seek WHERE $range SETTINGS $READ_CACHED" > /dev/null
done
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.filesystem_cache WHERE cache_name = '$CACHE_NAME'"

echo 'Full read after partial population'
${CLICKHOUSE_CLIENT} --query "$(agg_query)"

echo 'Cancelled queries in the middle of segment downloads'
for _ in 1 2 3 4; do
    ${CLICKHOUSE_CLIENT} --query "$(agg_query), max_execution_time = 0.01" > /dev/null 2>&1 &
done
wait

echo 'Concurrent overlapping reads and a merge'
out=$CLICKHOUSE_TMP/concurrent_out.txt
: > "$out"
for i in 1 2 3 4 5 6 7 8; do
    (
        case $((i % 3)) in
            0) ${CLICKHOUSE_CLIENT} --query "$(agg_query)" >> "$out" ;;
            1) ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(arraySum(a)) FROM t_seek WHERE x < 60000 SETTINGS $READ_CACHED" > /dev/null ;;
            2) ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(arraySum(b)) FROM t_seek WHERE x >= 40000 SETTINGS $READ_CACHED" > /dev/null ;;
        esac
    ) &
done
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_seek FINAL" > /dev/null &
wait
sort -u "$out" | grep -c ''
${CLICKHOUSE_CLIENT} --query "$(agg_query)"

echo 'Part data passes checksum verification after cached reads'
${CLICKHOUSE_CLIENT} --query "CHECK TABLE t_seek SETTINGS $READ_CACHED" | cut -f2

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_seek SYNC"
drop_filesystem_cache $CACHE_NAME
