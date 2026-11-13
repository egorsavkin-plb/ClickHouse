#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

CACHE_NAME='local_disk_cached'

# `MergeTreeReaderWide` calls `ReadBuffer::prefetch` on every column stream when
# `local_filesystem_read_prefetch` is on. With a filesystem cache on a local disk,
# the outer buffer is `CachedOnDiskReadBufferFromFile`, which now issues
# `POSIX_FADV_WILLNEED` on the cache segment file (kernel readahead is not
# observable from SQL), so this only checks that prefetched cached reads stay
# correct, cold and warm, single- and multi-threaded.

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_prefetch SYNC"
drop_filesystem_cache $CACHE_NAME

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_prefetch (x UInt64, a UInt64, b UInt64, c String)
    ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 'local_disk_cached'"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t_prefetch
    SELECT number, number * 3, number * 7, toString(number % 1000)
    FROM numbers(500000)"

QUERY="SELECT count(), sum(a), sum(b), sum(length(c)) FROM t_prefetch"

expected=$(${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS enable_filesystem_cache = 0, local_filesystem_read_prefetch = 1")

echo 'Cold read (read-through into cache) with prefetch'
got=$(${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS enable_filesystem_cache = 1, local_filesystem_read_prefetch = 1")
[ "$got" = "$expected" ] && echo OK || echo "MISMATCH: $expected vs $got"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.filesystem_cache WHERE cache_name = '$CACHE_NAME'"

echo 'Warm read (cache hits) with prefetch, single and multi-threaded'
got=$(${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS enable_filesystem_cache = 1, local_filesystem_read_prefetch = 1")
[ "$got" = "$expected" ] && echo OK || echo "MISMATCH: $expected vs $got"
got=$(${CLICKHOUSE_CLIENT} --query "$QUERY SETTINGS enable_filesystem_cache = 1, local_filesystem_read_prefetch = 1, max_threads = 8")
[ "$got" = "$expected" ] && echo OK || echo "MISMATCH: $expected vs $got"

echo 'Subset (seek) read with prefetch'
got=$(${CLICKHOUSE_CLIENT} --query "$QUERY WHERE x >= 100000 AND x < 400000 SETTINGS enable_filesystem_cache = 1, local_filesystem_read_prefetch = 1")
expected=$(${CLICKHOUSE_CLIENT} --query "$QUERY WHERE x >= 100000 AND x < 400000 SETTINGS enable_filesystem_cache = 0, local_filesystem_read_prefetch = 1")
[ "$got" = "$expected" ] && echo OK || echo "MISMATCH: $expected vs $got"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_prefetch SYNC"
drop_filesystem_cache $CACHE_NAME
