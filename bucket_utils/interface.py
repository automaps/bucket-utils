import abc
from datetime import datetime
from pathlib import Path

__all__ = ["ResumableUploadSessionInterface", "BucketInterface"]

from typing import List, Optional
from collections.abc import Iterable


class ResumableUploadSessionInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, url, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def get_url(self):
        raise NotImplementedError

    @abc.abstractmethod
    def upload(self, data):
        raise NotImplementedError


class BucketInterface(metaclass=abc.ABCMeta):
    """
    A bucket stores blobs.
    A blob is a file.
    """

    @abc.abstractmethod
    def __init__(self, bucket_name: str):
        raise NotImplementedError

    @abc.abstractmethod
    def upload_filepath(self, filepath: Path, blob: str):
        raise NotImplementedError

    @abc.abstractmethod
    def upload_file(self, file, blob: str):
        raise NotImplementedError

    def upload_from_string(self, string: str, blob: str):
        raise NotImplementedError

    @abc.abstractmethod
    def download_blob_to_filepath(self, filepath: Path, blob: str):
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def download_blobs_to_directory(self, blobs: List[str], directory: Path):
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, blob: str):
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, blob: str) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_blob_paths(
        self,
        prefix: Optional[str] = None,
        since_last_modified: Optional[datetime] = None,
    ) -> Iterable[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_blob_names(
        self,
        prefix: Optional[str] = None,
        since_last_modified: Optional[datetime] = None,
    ) -> Iterable[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_immediate_children(self, prefix: Optional[str] = None) -> Iterable[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def download_blob_as_text(self, blob: str) -> str:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def generate_signed_url(self, blob: str, **kwargs) -> str:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def create_resumable_upload_session(
        self, blob: str
    ) -> ResumableUploadSessionInterface:
        raise NotImplementedError

    @abc.abstractmethod
    def last_modified(self, blob: str) -> datetime:
        """
        Returns:
            datetime: Timezone aware datetime
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        raise NotImplementedError

    def crc32c_checksum(self, blob: str) -> str:
        """Returns the crc32c checksum of the blob"""
        raise NotImplementedError
