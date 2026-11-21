"""Private POSIX synthetic containers; descriptor-relative, single-owner I/O.

No Windows fallback: stdlib chmod does not establish a private Windows ACL.
All public filesystem failures are fixed categories. See docs/RECORDING.md for
the local-filesystem, cooperative-backend and trusted-owner threat model.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import stat
import time
import uuid

from .policy import StoragePolicy, integer
from .recorder import RecordingError


HEADER = b"SENTINEL-SYNTHETIC-V1\n"
FOOTER = b"\nSENTINEL-COMPLETE-V1\n"
OVERHEAD = len(HEADER) + len(FOOTER) + 8
NAMESPACE = "sentinel-recording-v1"
OWNED = re.compile(r"[0-9a-f]{32}\.(part|clip)\Z")


def _private(info: os.stat_result, *, directory: bool = False) -> None:
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if (not expected(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != (0o700 if directory else 0o600)):
        raise RecordingError("unsafe-storage")


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise RecordingError("storage-write-failed")
        view = view[written:]


class PrivateStore:
    """Existing owner-created 0700 root outside Git, on a local POSIX filesystem.

    Use as a context manager. A lifetime advisory lock excludes cooperating
    processes; the same-UID owner must not alter the reserved namespace.
    No recursion, payload-derived paths, background jobs or automatic root mkdir.
    """

    def __init__(self, root: Path, policy: StoragePolicy = StoragePolicy()) -> None:
        self._directory = -1
        self._lock = -1
        self._active: dict[str, int] = {}
        self._broken = False
        self._claimed = False
        if type(policy) is not StoragePolicy:
            raise RecordingError("invalid-storage-policy")
        self._policy = policy
        if (os.name != "posix" or not hasattr(os, "O_NOFOLLOW")
                or not hasattr(os, "O_DIRECTORY")):
            raise RecordingError("unsupported-storage-platform")
        root_fd = -1
        try:
            import fcntl
            path = Path(root)
            if not path.is_absolute() or ".." in path.parts:
                raise RecordingError("unsafe-storage-root")
            # Walk from / using pinned descriptors; never resolve symlinks.
            root_fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            for component in path.parts[1:]:
                self._outside_git(root_fd)
                info = os.fstat(root_fd)
                if (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX):
                    raise RecordingError("unsafe-storage-ancestor")
                next_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                  dir_fd=root_fd)
                os.close(root_fd)
                root_fd = next_fd
            self._outside_git(root_fd)
            _private(os.fstat(root_fd), directory=True)
            try:
                os.mkdir(NAMESPACE, mode=0o700, dir_fd=root_fd)
                os.fsync(root_fd)
            except FileExistsError:
                pass
            self._directory = os.open(NAMESPACE, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                      dir_fd=root_fd)
            _private(os.fstat(self._directory), directory=True)
            try:
                self._lock = os.open("owner.lock", os.O_RDWR | os.O_CREAT | os.O_EXCL
                                     | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self._directory)
            except FileExistsError:
                self._lock = os.open("owner.lock", os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK,
                                     dir_fd=self._directory)
            info = os.fstat(self._lock)
            _private(info)
            if info.st_nlink != 1 or info.st_size != 0:
                raise RecordingError("unsafe-storage-lock")
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.recover()
        except Exception:
            self._dispose()
            raise RecordingError("storage-unavailable") from None
        finally:
            if root_fd >= 0:
                os.close(root_fd)

    @staticmethod
    def _outside_git(fd: int) -> None:
        try:
            os.stat(".git", dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise RecordingError("unsafe-storage-root")

    def _ready(self) -> None:
        if self._directory < 0 or self._broken:
            raise RecordingError("storage-unavailable")

    def claim(self) -> None:
        """Bind this lifetime to one recorder; reopen after restart/recovery."""
        self._ready()
        if self._claimed or self._active:
            raise RecordingError("storage-owner-unavailable")
        self._claimed = True

    def _scan(self) -> dict[str, os.stat_result]:
        self._ready()
        records = {}
        self._entry_count = 0
        # Bound work and memory even if an owner accidentally adds many files.
        with os.scandir(self._directory) as entries:
            for count, entry in enumerate(entries, 1):
                self._entry_count = count
                if count > self._policy.max_entries:
                    raise RecordingError("storage-entry-capacity")
                if not OWNED.fullmatch(entry.name):
                    continue  # Unrelated entries are never opened or deleted.
                fd = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=self._directory)
                try:
                    info = os.fstat(fd)
                    _private(info)
                    if (info.st_nlink not in (1, 2)
                            or info.st_size > self._policy.max_bytes
                            or os.read(fd, len(HEADER)) != HEADER):
                        raise RecordingError("unsafe-storage-entry")
                    if entry.name.endswith(".clip"):
                        if info.st_size <= OVERHEAD:
                            raise RecordingError("invalid-completed-clip")
                        os.lseek(fd, -len(FOOTER) - 8, os.SEEK_END)
                        trailer = os.read(fd, len(FOOTER) + 8)
                        if (trailer[:len(FOOTER)] != FOOTER
                                or int.from_bytes(trailer[len(FOOTER):], "big")
                                != info.st_size - OVERHEAD):
                            raise RecordingError("invalid-completed-clip")
                    records[entry.name] = info
                finally:
                    os.close(fd)
        # Only the crash window between link and unlink may have two links.
        for name, info in records.items():
            peer = records.get(name[:-5] + (".clip" if name.endswith(".part") else ".part"))
            if info.st_nlink == 2 and (peer is None
                    or (peer.st_dev, peer.st_ino) != (info.st_dev, info.st_ino)):
                raise RecordingError("unsafe-storage-links")
        return records

    def recover(self) -> None:
        """Bounded/idempotent startup recovery; never promote partial files."""
        self._ready()
        if self._active:
            raise RecordingError("storage-busy")
        try:
            records = self._scan()  # Validate the entire bounded set first.
            for name in sorted(records):
                if name.endswith(".part"):
                    os.unlink(name, dir_fd=self._directory)
            os.fsync(self._directory)
            self._retain({name: info for name, info in records.items()
                          if name.endswith(".clip")})
        except Exception:
            self._broken = True
            raise RecordingError("storage-recovery-failed") from None

    def _retain(self, records: dict[str, os.stat_result]) -> dict[str, os.stat_result]:
        now = time.time_ns()
        age = int(self._policy.retention_seconds * 1_000_000_000)
        removed = 0
        for name, info in sorted(records.items(), key=lambda item: (item[1].st_mtime_ns, item[0])):
            if name.endswith(".clip") and now - info.st_mtime_ns >= age:
                os.unlink(name, dir_fd=self._directory)
                removed += 1
        os.fsync(self._directory)
        # Reconcile only confirmed, durable deletions from this bounded scan.
        # Unlink/fsync failures still abort admission and fence the store.
        self._entry_count -= removed
        return {name: info for name, info in records.items()
                if not (name.endswith(".clip") and now - info.st_mtime_ns >= age)}

    def begin(self, max_bytes: int) -> _Write:
        self._ready()
        integer(max_bytes, 1024 ** 3)
        try:
            records = self._retain(self._scan())
            clips = [info for name, info in records.items() if name.endswith(".clip")]
            if (any(name.endswith(".part") and name not in self._active for name in records)
                    or self._entry_count + 2 > self._policy.max_entries):
                raise RecordingError("storage-recovery-required")
            reserved = sum(self._active.values())
            if (len(clips) + len(self._active) >= self._policy.max_clips
                    or sum(info.st_size for info in clips) + reserved + max_bytes + OVERHEAD
                    > self._policy.max_bytes):
                raise RecordingError("storage-quota")
            name = uuid.uuid4().hex + ".part"
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=self._directory)
            self._active[name] = max_bytes + OVERHEAD
            transaction = _Write(self, name, fd, max_bytes)
            try:
                _private(os.fstat(fd))
                _write_all(fd, HEADER)
            except Exception:
                transaction.abort()
                raise
            return transaction
        except RecordingError:
            raise
        except Exception:
            self._broken = True
            raise RecordingError("storage-write-failed") from None

    def _dispose(self) -> None:
        for descriptor in (self._lock, self._directory):
            if descriptor >= 0:
                os.close(descriptor)
        self._lock = self._directory = -1

    def close(self) -> None:
        if self._active:
            raise RecordingError("storage-busy")
        self._dispose()

    def __enter__(self) -> PrivateStore:
        self._ready()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class _Write:
    def __init__(self, store: PrivateStore, name: str, fd: int, maximum: int) -> None:
        self._store = store
        self._name = name
        self._fd = fd
        self._maximum = maximum
        self._size = 0
        self._failed = False
        self._published = False
        self._done = False

    def write(self, data: bytes) -> None:
        self._store._ready()
        if (self._done or self._failed or self._published or type(data) is not bytes
                or len(data) > self._maximum - self._size):
            self._failed = True
            raise RecordingError("storage-write-bound")
        try:
            _write_all(self._fd, data)
            self._size += len(data)
        except Exception:
            self._failed = True
            raise RecordingError("storage-write-failed") from None

    async def complete(self) -> None:
        # Intentionally no await: cancellation is possible before this commit
        # starts or after it finishes, never halfway through Python's sequence.
        self._store._ready()
        if self._done or self._failed or self._size == 0:
            raise RecordingError("storage-incomplete")
        try:
            _write_all(self._fd, FOOTER + self._size.to_bytes(8, "big"))
            os.fsync(self._fd)
            # link() publishes exclusively; rename/replace could overwrite an
            # unrelated destination. Both names are on this pinned filesystem.
            os.link(self._name, self._name[:-5] + ".clip", src_dir_fd=self._store._directory,
                    dst_dir_fd=self._store._directory, follow_symlinks=False)
            self._published = True
            os.fsync(self._store._directory)
            os.unlink(self._name, dir_fd=self._store._directory)
            os.fsync(self._store._directory)
            self._release()
        except Exception:
            self._failed = True
            raise RecordingError("storage-finalization-failed") from None

    def _release(self) -> None:
        os.close(self._fd)
        self._fd = -1
        self._done = True
        self._store._active.pop(self._name, None)

    def abort(self) -> None:
        if self._done:
            return
        try:
            if self._published:
                # Commit may have succeeded; keep both valid names for recovery
                # and fence retries rather than produce a second completed clip.
                raise RecordingError("storage-finalization-uncertain")
            os.unlink(self._name, dir_fd=self._store._directory)
            os.fsync(self._store._directory)
        except Exception:
            self._store._broken = True
            raise RecordingError("storage-cleanup-failed") from None
        finally:
            self._release()
