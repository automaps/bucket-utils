import io
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Iterable

import crc32c
import pytz

from .exceptions import BlobNotFoundException
from .interface import BucketInterface, ResumableUploadSessionInterface

__all__ = ["LocalResumableUploadSession", "LocalBucket"]


def _ensure_parents(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def _upload(file, upload_path):
    # Start by deleting if it exists
    upload_path.unlink(missing_ok=True)

    _ensure_parents(upload_path)

    mode = "xb"
    if isinstance(file, io.BufferedReader):
        file = file.read()
    if isinstance(file, io.StringIO):
        file = file.read()
        mode = "x"
    with open(upload_path, mode) as blob_file:
        blob_file.write(file)


class LocalResumableUploadSession(ResumableUploadSessionInterface):
    def __init__(self, url, upload_blob_path: Path, **kwargs):
        self._url = url
        self.upload_blob_path = upload_blob_path

    def get_url(self):
        return self._url

    def upload(self, data):
        _upload(data, self.upload_blob_path)


def _expand_dirs(directories: List[Path]):
    """
    Returns all files in the list of directories given.
    Ignores any files given.
    """
    files = []
    for dir in directories:
        files.extend(dir.rglob("*"))
    return files


class LocalBucket(BucketInterface):
    """
    Mimics a cloud bucket but just uses a folder on the hosts filesystem
    """

    def __init__(self, bucket_name: str, directory: Path = None):
        if directory is None:
            directory = Path("/tmp/test_buckets/")
        self.bucket_name = bucket_name
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)

    def _ensure_parents(self, blob: str):
        (self.dir / blob).parent.mkdir(parents=True, exist_ok=True)

    def create_resumable_upload_session(self, blob: str):
        return LocalResumableUploadSession(
            f"https://mybucketapi.com/{self.bucket_name}/{blob}", self.dir / blob
        )

    def delete(self, blob: str):
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            (self.dir / blob).unlink()
        except FileNotFoundError:
            raise BlobNotFoundException(blob)

    def download_blob_as_text(self, blob: str) -> str:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            with open(self.dir / blob) as file:
                return file.read()
        except FileNotFoundError:
            raise BlobNotFoundException(blob)

    def download_blob_to_filepath(self, filepath: Path, blob: str):
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            shutil.copy(self.dir / blob, filepath)
        except FileNotFoundError:
            raise BlobNotFoundException(blob)

    def exists(self, blob: str):
        return (self.dir / blob).exists()

    def generate_signed_url(self, blob: str, **kwargs):
        return "https://test.com/signed-blob-upload-url"

    def iter_immediate_children(self, prefix: Optional[str] = None) -> Iterable[Path]:
        raise NotImplemented

    def _list_file_paths(
        self, prefix="", since_last_modified: Optional[datetime] = None
    ) -> List[Path]:
        where_prefix_is_a_directory = list((self.dir / prefix).rglob("*"))
        where_prefix_is_a_dir_or_file_name_prefix = list(self.dir.rglob(f"{prefix}*"))
        paths = [
            *where_prefix_is_a_directory,
            *where_prefix_is_a_dir_or_file_name_prefix,
        ]

        paths.extend(_expand_dirs(paths))

        # Remove duplicates
        paths = list(dict.fromkeys(paths))

        # Filter directories
        paths = [x for x in paths if x.is_file()]

        # Filter since_last_modified
        if since_last_modified:
            paths = self._remove_files_last_modified_before_time(
                paths, since_last_modified
            )

        return paths

    def _remove_files_last_modified_before_time(
        self, files: List[Path], time: datetime
    ) -> List[Path]:
        """
        Returns: List of files that were last updated after 'time'
        """
        result = []
        for file in files:
            if self.last_modified(file.name) >= time:
                result.append(file)
        return result

    def iter_blob_paths(
        self, prefix: Optional[str] = "", since_last_modified: Optional[datetime] = None
    ) -> List[str]:
        return [
            str(x.relative_to(self.dir))
            for x in self._list_file_paths(prefix, since_last_modified)
        ]

    def iter_blob_names(
        self, prefix: Optional[str] = "", since_last_modified: Optional[datetime] = None
    ) -> List[str]:
        return [x.name for x in self._list_file_paths(prefix, since_last_modified)]

    def upload_file(self, file, blob: str) -> None:
        _upload(file, self.dir / blob)

    def upload_filepath(self, filepath: Path, blob: str) -> None:
        _ensure_parents(self.dir / blob)
        with open(filepath, "rb") as source:
            self.upload_file(source, blob)

    def upload_from_string(self, string: str, blob: str) -> None:
        # Start by deleting if it exists
        blob_path = self.dir / blob
        blob_path.unlink(missing_ok=True)

        _ensure_parents(blob_path)
        with open(blob_path, "x") as file:
            file.write(string)

    def download_blobs_to_directory(self, blobs: List[str], directory: Path):
        for blob in blobs:
            self.download_blob_to_filepath(directory / blob, blob)

    def last_modified(self, blob: str) -> datetime:
        blob_path = self.dir / blob
        try:
            return datetime.fromtimestamp(blob_path.stat().st_mtime, tz=pytz.UTC)
        except FileNotFoundError:
            raise BlobNotFoundException(blob)

    def crc32c_checksum(self, blob: str) -> int:
        blob_path = self.dir / blob
        try:
            with blob_path.open("rb") as file:
                return crc32c.crc32c(file.read())
        except FileNotFoundError:
            raise BlobNotFoundException(blob)

    def delete_all(self):
        shutil.rmtree(self.dir, ignore_errors=True)
