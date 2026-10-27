#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./cache.lib
. "$CUR_DIR"/cache.lib

CACHE_NAME='local_disk_cached'
READ_CACHED="enable_filesystem_cache = 1"
READ_UNCACHED="enable_filesystem_cache = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cached_a SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cached_b SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cached_c SYNC"

drop_filesystem_cache $CACHE_NAME

echo 'Read-through populates the cache'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_cached_a (x UInt64, v UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 'local_disk_cached'"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_cached_a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cached_a SELECT number, number FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_a SETTINGS $READ_UNCACHED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_a SETTINGS $READ_CACHED"
${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.filesystem_cache WHERE cache_name = '$CACHE_NAME'"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_a SETTINGS $READ_CACHED"

echo 'Rewrite of a file at the same path is not served stale from the cache'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_cached_b (x UInt64, v UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 'local_disk_cached'"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_cached_b"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cached_b SELECT number, number + 100000 FROM numbers(10000)"

part_a=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_cached_a' AND name = 'all_1_1_0'")
part_b=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_cached_b' AND name = 'all_1_1_0'")
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cached_a DETACH PART 'all_1_1_0'"
cp -f "${part_b}"/* "$(dirname "$part_a")/detached/all_1_1_0/"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cached_a ATTACH PART 'all_1_1_0'"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR MARK CACHE"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR UNCOMPRESSED CACHE"
# t_cached_a now holds the data of t_cached_b (sum(v) = sum(number + 100000)).
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_a SETTINGS $READ_CACHED"

echo 'Mutation and merge after priming'
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_cached_a"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cached_a UPDATE v = v + 1 WHERE x < 10 SETTINGS mutations_sync = 1"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_cached_a FINAL"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_a SETTINGS $READ_CACHED"

echo 'Delete and recreate with the same row count'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_cached_c (x UInt64, v UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 'local_disk_cached'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cached_c SELECT number, number FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_c SETTINGS $READ_CACHED"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cached_c SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_cached_c (x UInt64, v UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS storage_policy = 'local_disk_cached'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cached_c SELECT number, number + 5000 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM t_cached_c SETTINGS $READ_CACHED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cached_a SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cached_b SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cached_c SYNC"
drop_filesystem_cache $CACHE_NAME
