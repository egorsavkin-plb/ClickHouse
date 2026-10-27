#include <Disks/DiskLocal.h>
#include <Common/IThrottler.h>
#include <Core/Defines.h>
#include <Common/createHardLink.h>
#include <Disks/DiskFactory.h>

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Common/atomicRename.h>
#include <Common/formatReadable.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadPipeline.h>
#include <Disks/loadLocalDiskConfig.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <filesystem>
#include <memory>
#include <system_error>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <Disks/IO/WriteBufferFromTemporaryFile.h>

#include <Common/randomSeed.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <pcg_random.hpp>
#include <Common/logger_useful.h>
#include <Common/ErrnoException.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/FileCache/FileCacheSettings.h>

#include <fmt/format.h>

// On illumos, <sys/regset.h> defines FS as a macro (x86 segment register).
// Undef it to allow use of the FS:: namespace from filesystemHelpers.h.
#ifdef FS
#  undef FS
#endif

namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_RMDIR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_STAT;
}

namespace FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsBool cache_on_write_operations;
}

namespace
{

/// Runs an invalidation callback when a write is finalized or cancelled, so that
/// bytes cached from a torn read during the write cannot outlive the write.
class CacheInvalidatingWriteBuffer final : public WriteBufferFromFileDecorator
{
public:
    CacheInvalidatingWriteBuffer(std::unique_ptr<WriteBufferFromFileBase> impl_, std::function<void()> invalidate_)
        : WriteBufferFromFileDecorator(std::move(impl_))
        , invalidate(std::move(invalidate_))
    {
    }

protected:
    void finalizeImpl() override
    {
        WriteBufferFromFileDecorator::finalizeImpl();
        invalidate();
    }

    void cancelImpl() noexcept override
    {
        WriteBufferFromFileDecorator::cancelImpl();
        try
        {
            invalidate();
        }
        catch (...)
        {
        }
    }

private:
    std::function<void()> invalidate;
};

struct timespec getMTime(const struct stat & file_stat)
{
#if defined(OS_DARWIN)
    return file_stat.st_mtimespec;
#else
    return file_stat.st_mtim;
#endif
}

struct timespec getCTime(const struct stat & file_stat)
{
#if defined(OS_DARWIN)
    return file_stat.st_ctimespec;
#else
    return file_stat.st_ctim;
#endif
}

bool errnoIndicatesReadOnlyDisk(int err)
{
    return err == EROFS || err == EACCES || err == EPERM || err == ENOSPC || err == EDQUOT;
}

UInt64 getTotalSpaceByName(const String & name, const String & disk_path, UInt64 keep_free_space_bytes)
{
    struct statvfs fs{};
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data" / "").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_blocks * fs.f_frsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

}

std::mutex DiskLocal::reservation_mutex;


using DiskLocalPtr = std::shared_ptr<DiskLocal>;

class DiskLocalReservation : public IReservation
{
public:
    DiskLocalReservation(const DiskLocalPtr & disk_, UInt64 size_, UInt64 unreserved_space_)
        : disk(disk_)
        , size(size_)
        , unreserved_space(unreserved_space_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {}

    UInt64 getSize() const override { return size; }
    std::optional<UInt64> getUnreservedSpace() const override { return unreserved_space; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't use i != 0 with single disk reservation. It's a bug");
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(DiskLocal::reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskLocalReservation() override
    {
        try
        {
            std::lock_guard lock(DiskLocal::reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                LOG_ERROR(getLogger("DiskLocal"), "Unbalanced reservations size for disk '{}'.", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
                LOG_ERROR(getLogger("DiskLocal"), "Unbalanced reservation count for disk '{}'.", disk->getName());
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    DiskLocalPtr disk;
    UInt64 size;
    UInt64 unreserved_space;
    CurrentMetrics::Increment metric_increment;
};


class DiskLocalDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskLocalDirectoryIterator() = default;
    DiskLocalDirectoryIterator(const String & disk_path_, const String & dir_path_)
        : dir_path(dir_path_), entry(fs::path(disk_path_) / dir_path_)
    {
    }

    void next() override { ++entry; }

    bool isValid() const override { return entry != fs::directory_iterator(); }

    String path() const override
    {
        if (entry->is_directory())
            return dir_path / entry->path().filename() / "";
        return dir_path / entry->path().filename();
    }

    String name() const override { return entry->path().filename(); }

private:
    fs::path dir_path;
    fs::directory_iterator entry;
};


ReservationPtr DiskLocal::reserve(UInt64 bytes)
{
    auto unreserved_space = tryReserve(bytes, std::nullopt);
    if (!unreserved_space.has_value())
        return {};
    return std::make_unique<DiskLocalReservation>(
        std::static_pointer_cast<DiskLocal>(shared_from_this()),
        bytes, unreserved_space.value());
}

ReservationPtr DiskLocal::reserve(UInt64 bytes, const ReservationConstraints & constraints)
{
    auto unreserved_space = tryReserve(bytes, constraints);
    if (!unreserved_space.has_value())
        return {};
    return std::make_unique<DiskLocalReservation>(
        std::static_pointer_cast<DiskLocal>(shared_from_this()),
        bytes, unreserved_space.value());
}

std::optional<UInt64> DiskLocal::tryReserve(UInt64 bytes, const std::optional<ReservationConstraints> & constraints)
{
    std::lock_guard lock(DiskLocal::reservation_mutex);

    auto available_space = getAvailableSpace();

    UInt64 unreserved_space = available_space
        ? *available_space - std::min(*available_space, reserved_bytes)
        : std::numeric_limits<UInt64>::max();

    /// Check constraints if specified
    if (constraints.has_value())
    {
        auto total_space = getTotalSpace();

        if (available_space.has_value() && total_space.has_value())
        {
            /// Not enough space for reservation itself
            if (bytes > unreserved_space)
                return {};

            UInt64 free_bytes_after = unreserved_space - bytes;

            /// Check min_bytes constraint
            if (constraints->min_bytes > 0 && free_bytes_after < constraints->min_bytes)
            {
                LOG_TRACE(logger, "Could not reserve {} ({} bytes) on disk {}. Free space after reservation {} bytes ({}) would be less than min_bytes {} bytes ({})",
                    ReadableSize(bytes), bytes, backQuote(name), free_bytes_after, ReadableSize(free_bytes_after), constraints->min_bytes, ReadableSize(constraints->min_bytes));
                return {};
            }

            /// Check min_ratio constraint
            if (constraints->min_ratio > 0.0f)
            {
                UInt64 min_bytes_from_ratio = static_cast<UInt64>(constraints->min_ratio * (static_cast<Float32>(*total_space)));
                if (free_bytes_after < min_bytes_from_ratio)
                {
                    LOG_TRACE(logger, "Could not reserve {} ({} bytes) on disk {}. Free space after reservation {} bytes ({}) would be less than min_ratio requirement {} bytes ({}) (ratio: {}, total: {} bytes)",
                        ReadableSize(bytes), bytes, backQuote(name), free_bytes_after, ReadableSize(free_bytes_after), min_bytes_from_ratio, ReadableSize(min_bytes_from_ratio), constraints->min_ratio, *total_space);
                    return {};
                }
            }
        }
    }

    if (bytes == 0)
    {
        LOG_TRACE(logger, "Reserved 0 bytes on local disk {}", backQuote(name));
        ++reservation_count;
        return {unreserved_space};
    }

    if (unreserved_space >= bytes)
    {
        if (available_space)
        {
            LOG_TRACE(
                logger,
                "Reserved {} on local disk {}, having unreserved {}.",
                ReadableSize(bytes),
                backQuote(name),
                ReadableSize(unreserved_space));
        }
        else
        {
            LOG_TRACE(
                logger,
                "Reserved {} on local disk {}.",
                ReadableSize(bytes),
                backQuote(name));
        }

        ++reservation_count;
        reserved_bytes += bytes;
        return {unreserved_space - bytes};
    }

    LOG_TRACE(logger, "Could not reserve {} on local disk {}. Not enough unreserved space", ReadableSize(bytes), backQuote(name));

    return {};
}

std::optional<UInt64> DiskLocal::getTotalSpace() const
{
    if (broken || readonly)
        return 0;
    return getTotalSpaceByName(name, disk_path, keep_free_space_bytes);
}

std::optional<UInt64> DiskLocal::getAvailableSpace() const
{
    if (broken || readonly)
        return 0;
    /// we use f_bavail, because part of b_free space is
    /// available for superuser only and for system purposes
    struct statvfs fs{};
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data" / "").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_bavail * fs.f_frsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

std::optional<UInt64> DiskLocal::getUnreservedSpace() const
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    auto available_space = *getAvailableSpace();
    available_space -= std::min(available_space, reserved_bytes);
    return available_space;
}

bool DiskLocal::existsFileOrDirectory(const String & path) const
{
    return existsOrFileNameTooLong([&] { return fs::exists(fs::path(disk_path) / path); });
}

bool DiskLocal::existsFile(const String & path) const
{
    return existsOrFileNameTooLong([&] { return fs::is_regular_file(fs::path(disk_path) / path); });
}

bool DiskLocal::existsDirectory(const String & path) const
{
    return existsOrFileNameTooLong([&] { return fs::is_directory(fs::path(disk_path) / path); });
}

size_t DiskLocal::getFileSize(const String & path) const
{
    return fs::file_size(fs::path(disk_path) / path);
}

void DiskLocal::createDirectory(const String & path)
{
    fs::create_directory(fs::path(disk_path) / path);
}

void DiskLocal::createDirectories(const String & path)
{
    fs::create_directories(fs::path(disk_path) / path);
}

void DiskLocal::moveDirectory(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

DirectoryIteratorPtr DiskLocal::iterateDirectory(const String & path) const
{
    fs::path meta_path = fs::path(disk_path) / path;
    if (!broken && fs::exists(meta_path) && fs::is_directory(meta_path))
        return std::make_unique<DiskLocalDirectoryIterator>(disk_path, path);
    return std::make_unique<DiskLocalDirectoryIterator>();
}

void DiskLocal::moveFile(const String & from_path, const String & to_path)
{
    /// No cache invalidation: the destination did not exist, and the version key
    /// follows the inode, so an existing entry stays correct under the new path.
    /// The same applies to `moveDirectory`, `renameExchange` and `createHardLink`.
    renameNoReplace(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

void DiskLocal::replaceFile(const String & from_path, const String & to_path)
{
    /// The destination inode is replaced and its number can be reused later.
    invalidateCacheEntry(to_path);
    fs::path from_file = fs::path(disk_path) / from_path;
    fs::path to_file = fs::path(disk_path) / to_path;
    fs::create_directories(to_file.parent_path());
    fs::rename(from_file, to_file);
}

void DiskLocal::renameExchange(const std::string & old_path, const std::string & new_path)
{
    DB::renameExchange(fs::path(disk_path) / old_path, fs::path(disk_path) / new_path);
}

bool DiskLocal::renameExchangeIfSupported(const std::string & old_path, const std::string & new_path)
{
    return DB::renameExchangeIfSupported(fs::path(disk_path) / old_path, fs::path(disk_path) / new_path);
}

FileCacheKey DiskLocal::makeFileCacheKey(const struct stat & file_stat)
{
    const auto mtime = getMTime(file_stat);
    const auto ctime = getCTime(file_stat);
    return FileCacheKey::fromPath(fmt::format(
        "local:{}:{}:{}:{}:{}:{}:{}",
        static_cast<UInt64>(file_stat.st_dev),
        static_cast<UInt64>(file_stat.st_ino),
        static_cast<UInt64>(file_stat.st_size),
        static_cast<Int64>(mtime.tv_sec),
        static_cast<Int64>(mtime.tv_nsec),
        static_cast<Int64>(ctime.tv_sec),
        static_cast<Int64>(ctime.tv_nsec)));
}

void DiskLocal::invalidateCacheEntry(const struct stat & file_stat) const
{
    if (!filesystem_cache || !filesystem_cache->isInitialized() || !S_ISREG(file_stat.st_mode))
        return;
    filesystem_cache->removeKeyIfExists(makeFileCacheKey(file_stat), FileCache::getCommonOrigin().user_id);
}

void DiskLocal::invalidateCacheEntry(const String & path) const
{
    if (!filesystem_cache)
        return;
    struct stat file_stat{};
    if (0 == ::stat((fs::path(disk_path) / path).c_str(), &file_stat))
        invalidateCacheEntry(file_stat);
}

void DiskLocal::invalidateCacheTree(const String & path) const
{
    if (!filesystem_cache)
        return;
    auto full_path = fs::path(disk_path) / path;
    struct stat file_stat{};
    if (0 != ::stat(full_path.c_str(), &file_stat))
        return;
    if (S_ISREG(file_stat.st_mode))
    {
        invalidateCacheEntry(file_stat);
        return;
    }
    if (!S_ISDIR(file_stat.st_mode))
        return;
    std::error_code ec;
    for (const auto & entry : fs::recursive_directory_iterator(full_path, ec))
    {
        struct stat entry_stat{};
        if (0 == ::stat(entry.path().c_str(), &entry_stat))
            invalidateCacheEntry(entry_stat);
    }
}

void DiskLocal::prepareRead(
    const String & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    ReadPipeline & pipeline) const
{
    auto full_path = fs::path(disk_path) / path;

    /// Do not fail eagerly if the file doesn't exist or can't be stat'd.
    /// The error should come at read time with the proper error code
    /// (e.g. FILE_DOESNT_EXIST for broken projections). Mark the size unknown in
    /// that case so the reader opens the file rather than treating a zero size as
    /// an empty file.
    std::error_code ec;
    auto file_size = fs::file_size(full_path, ec);

    StoredObject obj(full_path.string(), full_path.string(), ec ? StoredObject::UnknownSize : file_size);

    /// Filesystem cache (on a second, faster disk), for regular non-empty files.
    struct stat file_stat{};
    bool use_filesystem_cache = filesystem_cache && filesystem_cache->isInitialized()
        && settings.enable_filesystem_cache
        && 0 == ::stat(full_path.c_str(), &file_stat) && S_ISREG(file_stat.st_mode) && file_stat.st_size > 0;

    if (use_filesystem_cache)
    {
        /// The cached reader drives its impl via `seek`/`set`, which the async and
        /// mmap local readers do not support, and O_DIRECT alignment is not
        /// guaranteed for it. Force the synchronous reader underneath the cache,
        /// the same way `LocalObjectStorage::patchSettings` does for the
        /// object-storage local backend. Without this the cache would never
        /// engage, because the default local read method is `pread_threadpool`.
        auto cached_settings = settings;
        cached_settings.local_fs_settings.method = LocalFSReadMethod::pread;
        cached_settings.local_fs_settings.direct_io_threshold = 0;

        pipeline.setLocalFileSource(
            full_path.string(),
            StoredObjects{obj},
            cached_settings,
            read_hint);

        std::shared_ptr<FilesystemCacheLog> cache_log;
        if (auto global_context = Context::getGlobalContextInstance())
            cache_log = global_context->getFilesystemCacheLog();

        pipeline.needFilesystemCache(
            filesystem_cache,
            makeFileCacheKey(file_stat),
            filesystem_cache->getCommonOriginWithSegmentKeyType(full_path.string()),
            cached_settings.filesystem_cache_settings,
            cache_log);

        /// The filesystem cache replaces the RAM page cache for this disk:
        /// stacking both would keep the same bytes in RAM and on the cache disk.
        return;
    }

    /// No gather for local disk — the source buffer is returned directly.
    pipeline.setLocalFileSource(
        full_path.string(),
        StoredObjects{obj},
        settings,
        read_hint);

    /// Page cache is incompatible with several local read methods:
    ///   - async methods (io_uring, pread_fake_async, pread_threadpool): the
    ///     async wrapper drives the inner reader incompatibly with page-cache
    ///     `set`/`seek` semantics;
    ///   - mmap: `MMapReadBufferFromFileWithCache::seek` bounds-checks against
    ///     `working_buffer.size()` after `CachedInMemoryReadBufferFromFile` has
    ///     shrunk it to the page-cache piece, so any seek past the first piece
    ///     throws `CANNOT_SEEK_THROUGH_FILE`;
    ///   - O_DIRECT when `page_cache_block_size` is not aligned to the direct
    ///     IO sector size (additional check below).
    bool use_page_cache = settings.use_page_cache_for_local_disks && settings.page_cache_settings.cache
        && settings.local_fs_settings.method != LocalFSReadMethod::io_uring
        && settings.local_fs_settings.method != LocalFSReadMethod::pread_fake_async
        && settings.local_fs_settings.method != LocalFSReadMethod::pread_threadpool
        && settings.local_fs_settings.method != LocalFSReadMethod::mmap;

    {
        /// Legacy pipeline: selection is read_hint-based (0 when absent), so falling back to
        /// file_size is a conservative upper bound when there is no hint. The optional reader
        /// executor is different: LocalSourceReader selects by file_size and ignores read_hint,
        /// so this expression is not an upper bound for that path (a small hint can hide a large
        /// file). This check predates the current change; it only decides whether the userspace
        /// page cache may coexist with an unaligned page-cache block size.
        size_t estimated_size = read_hint.value_or(file_size);
        if (use_page_cache && settings.local_fs_settings.direct_io_threshold
            && estimated_size >= settings.local_fs_settings.direct_io_threshold
            && settings.page_cache_settings.block_size % DEFAULT_AIO_FILE_BLOCK_SIZE != 0)
            use_page_cache = false;
    }

    if (use_page_cache)
        pipeline.needMemoryCache("local:", settings.page_cache_settings);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskLocal::writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings)
{
    /// Drop the pre-write version before the file is opened (truncated/appended to),
    /// and again on finalize/cancel via the wrapper: a concurrent reader may have
    /// re-cached torn bytes under a key that becomes current again.
    invalidateCacheEntry(path);

    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    auto buffer = std::make_unique<WriteBufferFromFile>(
        fs::path(disk_path) / path,
        buf_size,
        flags,
        settings.local_throttler,
        0666,
        nullptr,
        0,
        settings.use_adaptive_write_buffer,
        settings.adaptive_write_buffer_initial_size);

    if (!filesystem_cache)
        return buffer;

    return std::make_unique<CacheInvalidatingWriteBuffer>(
        std::move(buffer), [this, path]() { invalidateCacheEntry(path); });
}

std::vector<String> DiskLocal::getBlobPath(const String & path) const
{
    auto fs_path = fs::path(disk_path) / path;
    return {fs_path};
}

void DiskLocal::writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    invalidateCacheEntry(path);
    auto fs_path = fs::path(disk_path) / path;
    try
    {
        std::move(write_blob_function)({fs_path}, mode, {});
    }
    catch (...)
    {
        invalidateCacheEntry(path);
        throw;
    }
    invalidateCacheEntry(path);
}

void DiskLocal::removeFile(const String & path)
{
    /// Invalidate before unlink: after it, the inode number can be reused by a
    /// recreated file whose version key could otherwise collide with this entry.
    invalidateCacheEntry(path);
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, fs_path, "Cannot unlink file {}", fs_path);
}

void DiskLocal::removeFileIfExists(const String & path)
{
    invalidateCacheEntry(path);
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
    {
        if (errno != ENOENT)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, fs_path, "Cannot unlink file {}", fs_path);
    }
}

void DiskLocal::removeDirectory(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != rmdir(fs_path.c_str()))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_RMDIR, fs_path, "Cannot remove directory {}", fs_path);
}

void DiskLocal::removeDirectoryIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (!existsDirectory(fs_path))
        return;
    if (0 != rmdir(fs_path.c_str()))
        if (errno != ENOENT)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_RMDIR, fs_path, "Cannot remove directory {}", fs_path);
}

void DiskLocal::removeRecursive(const String & path)
{
    invalidateCacheTree(path);
    (void)fs::remove_all(fs::path(disk_path) / path);
}

void DiskLocal::listFiles(const String & path, std::vector<String> & file_names) const
{
    file_names.clear();
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        file_names.emplace_back(entry.path().filename());
}

void DiskLocal::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    /// The mtime is part of the version key: drop the entry for the old stamp.
    invalidateCacheEntry(path);
    FS::setModificationTime(fs::path(disk_path) / path, timestamp.epochTime());
}

Poco::Timestamp DiskLocal::getLastModified(const String & path) const
{
    return FS::getModificationTimestamp(fs::path(disk_path) / path);
}

time_t DiskLocal::getLastChanged(const String & path) const
{
    return FS::getChangeTime(fs::path(disk_path) / path);
}

void DiskLocal::createHardLink(const String & src_path, const String & dst_path)
{
    DB::createHardLink(fs::path(disk_path) / src_path, fs::path(disk_path) / dst_path);
}

bool DiskLocal::isSymlink(const String & path) const
{
    return FS::isSymlink(fs::path(disk_path) / path);
}

bool DiskLocal::isSymlinkNoThrow(const String & path) const
{
    return FS::isSymlinkNoThrow(fs::path(disk_path) / path);
}

void DiskLocal::createDirectorySymlink(const String & target, const String & link)
{
    auto link_path_inside_disk = fs::path(disk_path) / link;
    /// Symlinks will be relative.
    fs::create_directory_symlink(fs::proximate(fs::path(disk_path) / target, link_path_inside_disk.parent_path()), link_path_inside_disk);
}

String DiskLocal::readSymlink(const fs::path & path) const
{
    return FS::readSymlink(fs::path(disk_path) / path);
}

bool DiskLocal::equivalent(const String & p1, const String & p2) const
{
    return fs::equivalent(fs::path(disk_path) / p1, fs::path(disk_path) / p2);
}

bool DiskLocal::equivalentNoThrow(const String & p1, const String & p2) const
{
    std::error_code ec;
    return fs::equivalent(fs::path(disk_path) / p1, fs::path(disk_path) / p2, ec);
}

void DiskLocal::truncateFile(const String & path, size_t size)
{
    invalidateCacheEntry(path);
    int res = truncate((fs::path(disk_path) / path).string().data(), size);
    if (-1 == res)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_TRUNCATE_FILE, path, "Cannot truncate {}", path);
}

void DiskLocal::createFile(const String & path)
{
    FS::createFile(fs::path(disk_path) / path);
}

void DiskLocal::setReadOnly(const String & path)
{
    fs::permissions(fs::path(disk_path) / path,
                    fs::perms::owner_write | fs::perms::group_write | fs::perms::others_write,
                    fs::perm_options::remove);
}

bool inline isSameDiskType(const IDisk & one, const IDisk & another)
{
    return typeid(one) == typeid(another);
}

void DiskLocal::copyDirectoryContent(
    const String & from_dir,
    const std::shared_ptr<IDisk> & to_disk,
    const String & to_dir,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook)
{
    /// If throttling was configured we cannot use copying directly.
    if (isSameDiskType(*this, *to_disk) && !read_settings.local_throttler && !write_settings.local_throttler)
    {
        /// The fast path bypasses `writeFile` of the destination disk, so drop its
        /// cached entries for the overwritten tree upfront.
        if (auto * local_to_disk = static_cast<DiskLocal *>(to_disk.get()); local_to_disk->filesystem_cache)
            local_to_disk->invalidateCacheTree(to_dir);
        fs::copy(fs::path(disk_path) / from_dir, fs::path(to_disk->getPath()) / to_dir, fs::copy_options::recursive | fs::copy_options::overwrite_existing); /// Use more optimal way.
    }
    else
        IDisk::copyDirectoryContent(from_dir, to_disk, to_dir, read_settings, write_settings, cancellation_hook);
}

SyncGuardPtr DiskLocal::getDirectorySyncGuard(const String & path) const
{
    return std::make_unique<LocalDirectorySyncGuard>(fs::path(disk_path) / path);
}


void DiskLocal::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap & disk_map)
{
    /// Adding or removing the cache changes the disk's read path and cannot be
    /// applied without recreating the disk; cache setting changes on an existing
    /// cache are applied by `FileCacheFactory::updateSettingsFromConfig`.
    if (config.has(config_prefix + ".filesystem_cache") != (filesystem_cache != nullptr))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Filesystem cache of disk {} cannot be added or removed without recreating the disk", name);

    String new_disk_path;
    UInt64 new_keep_free_space_bytes = 0;

    loadDiskLocalConfig(name, config, config_prefix, context, new_disk_path, new_keep_free_space_bytes);

    if (disk_path != new_disk_path)
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path can't be updated from config {}", name);

    if (keep_free_space_bytes != new_keep_free_space_bytes)
        keep_free_space_bytes = new_keep_free_space_bytes;

    IDisk::applyNewSettings(config, context, config_prefix, disk_map);
}

DiskLocal::DiskLocal(const String & name_, const String & path_, UInt64 keep_free_space_bytes_,
                     const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
                     FileCachePtr filesystem_cache_, String filesystem_cache_name_)
    : IDisk(name_, config, config_prefix)
    , disk_path(path_)
    , filesystem_cache(std::move(filesystem_cache_))
    , filesystem_cache_name(std::move(filesystem_cache_name_))
    , keep_free_space_bytes(keep_free_space_bytes_)
    , logger(getLogger("DiskLocal"))
    , data_source_description(getLocalDataSourceDescription(disk_path))
{
    data_source_description.is_cached = filesystem_cache != nullptr;
}

DiskLocal::DiskLocal(
    const String & name_, const String & path_, UInt64 keep_free_space_bytes_, ContextPtr context,
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
    FileCachePtr filesystem_cache_, String filesystem_cache_name_)
    : DiskLocal(name_, path_, keep_free_space_bytes_, config, config_prefix, std::move(filesystem_cache_), std::move(filesystem_cache_name_))
{
    auto local_disk_check_period_ms = config.getUInt("local_disk_check_period_ms", 0);
    if (local_disk_check_period_ms > 0)
        disk_checker.emplace(this, context, local_disk_check_period_ms);
}

DiskLocal::DiskLocal(const String & name_, const String & path_)
    : IDisk(name_)
    , disk_path(path_)
    , keep_free_space_bytes(0)
    , logger(getLogger("DiskLocal"))
    , data_source_description(getLocalDataSourceDescription(disk_path))
{
}

DataSourceDescription DiskLocal::getDataSourceDescription() const
{
    return data_source_description;
}

DataSourceDescription DiskLocal::getLocalDataSourceDescription(const String & path)
{
    DataSourceDescription res;
    res.type = DataSourceType::Local;

    if (auto block_device_id = tryGetBlockDeviceId(path); block_device_id.has_value())
        res.description = *block_device_id;
    else
        res.description = path;
    res.is_encrypted = false;
    res.is_cached = false;
    return res;
}

void DiskLocal::shutdown()
{
    if (disk_checker)
        disk_checker->shutdown();
}

void DiskLocal::checkAccessImpl(const String & path)
{
    if (!FS::canRead(disk_path, /*allow_throw=*/false))
    {
        broken = true;
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "There is no read access to disk {} ({}).", name, disk_path);
    }

    if (!FS::canWrite(disk_path, /*allow_throw=*/false))
    {
        if (!readonly.exchange(true))
            LOG_INFO(logger, "Cannot write to the root directory of disk {} ({}).", name, disk_path);

        broken = false;
        return;
    }

    try
    {
        IDisk::checkAccessImpl(path);
    }
    catch (const ErrnoException & e)
    {
        if (errnoIndicatesReadOnlyDisk(e.getErrno()))
        {
            if (!readonly.exchange(true))
                LOG_INFO(logger, "Cannot write to disk {} ({}): {}", name, disk_path, getCurrentExceptionMessage(/* with_stacktrace = */ false));

            broken = false;
            return;
        }

        broken = true;
        throw;
    }
    catch (...)
    {
        broken = true;
        throw;
    }

    broken = false;
    readonly = false;
}

void DiskLocal::setup()
{
    fs::create_directories(disk_path);
}

void DiskLocal::startupImpl()
{
    broken = false;

    if (filesystem_cache)
        filesystem_cache->initialize();

    try
    {
        setup();
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Disk {} is marked as broken during startup", name));
        broken = true;
    }

    if (disk_checker)
        disk_checker->startup();
}

struct stat DiskLocal::stat(const String & path) const
{
    struct stat st{};
    auto full_path = fs::path(disk_path) / path;
    if (::stat(full_path.string().c_str(), &st) == 0)
        return st;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_STAT, path, "Cannot stat file: {}", path);
}

void DiskLocal::chmod(const String & path, mode_t mode)
{
    /// `chmod` updates ctime, which is part of the version key.
    invalidateCacheEntry(path);
    auto full_path = fs::path(disk_path) / path;
    if (::chmod(full_path.string().c_str(), mode) == 0)
        return;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot chmod file: {}", path);
}

ObjectStoragePtr DiskLocal::getObjectStorage()
{
    LocalObjectStorageSettings settings_object_storage(name, disk_path, /* read_only */false);
    return std::make_shared<LocalObjectStorage>(settings_object_storage);
}

void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check);
void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool attach,
        bool custom_disk) -> DiskPtr
    {
        String path;
        UInt64 keep_free_space_bytes = 0;
        loadDiskLocalConfig(name, config, config_prefix, context, path, keep_free_space_bytes);

        if (custom_disk && !attach)
            checkCustomLocalDiskPath(path, context);

        for (const auto & [disk_name, disk_ptr] : map)
            if (path == disk_ptr->getPath())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} and disk {} cannot have the same path ({})", name, disk_name, path);

        /// Optional filesystem cache on a second (faster) disk, configured as a
        /// nested section of the disk itself:
        /// <filesystem_cache><path>/nvme/cache/</path><max_size>...</max_size></filesystem_cache>
        /// Only for disks from server configuration: a cache created from a query
        /// would silently outlive the table definition that created it.
        FileCachePtr filesystem_cache;
        String filesystem_cache_name;
        const String filesystem_cache_prefix = config_prefix + ".filesystem_cache";
        if (config.has(filesystem_cache_prefix))
        {
            if (custom_disk)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Filesystem cache for disk {} of type `local` can be configured only in server configuration", name);

            FileCacheSettings file_cache_settings;
            file_cache_settings.loadFromConfig(
                config, filesystem_cache_prefix, getPathPrefixForRelativeCachePath(context), /* default_cache_path */ "");

            /// Writes go to the local disk and only invalidate; they never populate.
            if (file_cache_settings[FileCacheSetting::cache_on_write_operations].value)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Filesystem cache for disk {} of type `local` is read-through only, `cache_on_write_operations` is not supported", name);

            const String cache_path = file_cache_settings[FileCacheSetting::path].value;
            auto normalized = [](const String & p) { return fs::weakly_canonical(fs::path(p)).string(); };
            const String normalized_disk_path = normalized(path);
            const String normalized_cache_path = normalized(cache_path);
            if (normalized_cache_path == normalized_disk_path
                || pathStartsWith(normalized_cache_path, normalized_disk_path)
                || pathStartsWith(normalized_disk_path, normalized_cache_path))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Filesystem cache path ({}) of disk {} must not lie inside the disk path ({}) or vice versa",
                    cache_path, name, path);

            filesystem_cache_name = name;
            filesystem_cache = FileCacheFactory::instance().getOrCreate(
                filesystem_cache_name, file_cache_settings, filesystem_cache_prefix);
        }

        bool skip_access_check = config.getBool(config_prefix + ".skip_access_check", false) || global_skip_access_check;
        std::shared_ptr<IDisk> disk
            = std::make_shared<DiskLocal>(name, path, keep_free_space_bytes, context, config, config_prefix,
                std::move(filesystem_cache), std::move(filesystem_cache_name));
        disk->startup(skip_access_check);
        return disk;
    };
    factory.registerDiskType("local", creator, Documentation{
        .description = "Stores data on the local filesystem at a configured path. This is the default disk used when no disks are configured. An optional nested `filesystem_cache` section (server configuration only) caches reads on a second, faster disk.",
        .syntax = "disk(type = local, path = '/var/lib/clickhouse/disk_local/')",
        .related = {"object_storage", "cache", "encrypted"}});
}

}
