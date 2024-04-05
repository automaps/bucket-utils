import base64
import logging
import struct
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import google
from google.cloud import storage
from google.oauth2 import service_account

from .exceptions import BlobNotFoundException
from .interface import BucketInterface, ResumableUploadSessionInterface
from .resumable_upload_session import HTTPResumableUploadSession

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel("INFO")

__all__ = ["GoogleCloudBucket"]


def _prepend_work_dir(
    work_dir: Optional[str], directory: Optional[str]
) -> Optional[str]:
    if work_dir is None and directory is None:
        return None
    if work_dir is None:
        return directory
    if directory is None:
        return work_dir

    return f"{work_dir}/{directory}"


class GoogleCloudBucket(BucketInterface):
    def __init__(self, bucket_name: str, work_dir: str = ""):
        """
        :param bucket_name: The name of the bucket to use.
        :param work_dir: Optional work_dir to store and load all blobs from.
        Can be used for environment segregation purposes.
        e.g. work_dir=tests, will prepend all blob_path strings with "tests/"
        """
        self._bucket = None
        self.bucket_name = bucket_name
        if work_dir != "" and work_dir.endswith("/"):
            work_dir = f"{work_dir}/"
        self._work_dir = work_dir  # Either empty string or string that ends with slash

    def _construct_blob(self, blob_path: str) -> storage.Blob:
        """
        Construct a client side representation of a blob.
        Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        any content from Google Cloud Storage. As we don't need additional data,
        using `Bucket.blob` is preferred here.


        :param blob_path: str
        :return: The blob object created.
        """
        bucket = self._get_bucket()

        path = blob_path
        if self._work_dir is not None:
            path = f"{self._work_dir}{blob_path}"  # Inserts the slash
        blob = bucket.blob(path)
        return blob

    def _get_bucket(self) -> storage.Bucket:
        if self._bucket is None:
            storage_client = storage.Client(project="deployment-composer-test")
            self._bucket = storage_client.get_bucket(self.bucket_name)
        return self._bucket

    def _list_blobs(
        self,
        prefix: Optional[str] = None,
        since_last_modified: Optional[datetime] = None,
    ) -> Iterable[storage.Blob]:
        bucket = self._get_bucket()

        if prefix is None:
            prefix = ""

        blobs: List[storage.Blob] = list(
            bucket.list_blobs(prefix=f"{self._work_dir}{prefix}")
        )
        if since_last_modified is not None:
            blobs = self._remove_blobs_last_modified_before_time(
                blobs, since_last_modified
            )

        return blobs

    def _remove_blobs_last_modified_before_time(
        self, blobs: List[storage.Blob], time: datetime
    ) -> List[storage.Blob]:
        result = []
        for blob in blobs:
            if self.last_modified(blob.name) >= time:
                result.append(blob)
        return result

    def upload_filepath(self, filepath: Path, blob: str) -> None:
        """Uploads a file to the bucket."""
        blob_obj = self._construct_blob(blob)

        blob_obj.upload_from_filename(filepath.as_posix())

        LOGGER.debug(f"File {filepath} uploaded to {blob}.")

    def download_blob_to_filepath(self, filepath: Path, blob: str) -> None:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            blob_obj = self._construct_blob(blob)
            blob_obj.download_to_filename(filepath.as_posix())

            LOGGER.debug(
                f"Downloaded storage object {blob} from bucket {self.bucket_name} to local file {filepath}."
            )
        except google.cloud.exceptions.NotFound:
            raise BlobNotFoundException(blob)

    def delete(self, blob: str) -> None:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            blob_obj = self._construct_blob(blob)
            blob_obj.delete()

            LOGGER.debug(
                f"Deleted storage object {blob} from bucket {self.bucket_name}."
            )
        except google.cloud.exceptions.NotFound:
            raise BlobNotFoundException(blob)

    def exists(self, blob: str) -> bool:
        blob_obj = self._construct_blob(blob)
        return blob_obj.exists()

    def delete_work_dir(self) -> None:
        if self._work_dir == "":
            LOGGER.warning(f"WONT DELETE THE ROOT BLOB_DIR OF {self.bucket_name}")
        blobs = self._list_blobs(prefix=self._work_dir)
        for blob in blobs:
            blob.delete()

    def iter_blob_paths(
        self,
        prefix: Optional[str] = None,
        since_last_modified: Optional[datetime] = None,
    ) -> Iterable[str]:
        for b in self._list_blobs(prefix, since_last_modified):
            yield b.name.removeprefix(self._work_dir)

    def iter_blob_names(
        self,
        prefix: Optional[str] = None,
        since_last_modified: Optional[datetime] = None,
    ) -> Iterable[str]:
        for b in self._list_blobs(prefix, since_last_modified):
            yield b.name.rsplit("/", 1)[-1]

    def iter_immediate_children(self, prefix: Optional[str] = None) -> Iterable[str]:
        if prefix is None:
            prefix = ""

        blobs: List[storage.Blob] = list(
            self._list_blobs(prefix=f"{self._work_dir}{prefix}")
        )

        children = {
            blob.name.removeprefix(f"{self._work_dir}{prefix}/").split("/")[0]
            for blob in blobs
        }

        yield from children

    def upload_file(self, file, blob: str) -> None:
        blob_obj = self._construct_blob(blob)
        blob_obj.upload_from_file(file)

        LOGGER.debug(f"File uploaded to {blob}")

    def upload_from_string(self, string: str, blob: str) -> None:
        blob_obj = self._construct_blob(blob)
        blob_obj.upload_from_string(string)

        LOGGER.debug(f"String uploaded to {blob}")

    def download_blob_as_text(self, blob: str) -> str:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            blob_obj = self._construct_blob(blob)
            return blob_obj.download_as_text()
        except google.cloud.exceptions.NotFound:
            raise BlobNotFoundException(blob)

    def generate_signed_url(self, blob: str, **kwargs) -> str:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        # Remove this when we figure out how to do proper authentication.
        # Right now generate_signed_url() might be the only thing stopping us from using default gcloud authentication:
        # https://stackoverflow.com/a/57964111/15175103
        path_to_signing_key = kwargs["path_to_signing_key"]
        if not path_to_signing_key:
            raise Exception(
                'Keyword argument "path_to_signing_key" required to generate a signed url'
            )
        credentials = service_account.Credentials.from_service_account_file(
            path_to_signing_key
        )

        expiration = timedelta(days=1)  # TODO: Maybe add this as a optional parameter
        try:
            blob_obj = self._construct_blob(blob)
            return blob_obj.generate_signed_url(expiration, credentials=credentials)
        except google.cloud.exceptions.NotFound:
            raise BlobNotFoundException(blob)

    def create_resumable_upload_session(
        self, blob: str
    ) -> ResumableUploadSessionInterface:
        blob_obj = self._construct_blob(blob)
        url = blob_obj.create_resumable_upload_session()
        return HTTPResumableUploadSession(url)

    def download_blobs_to_directory(self, blobs: List[str], directory: Path) -> None:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        for blob in blobs:
            self.download_blob_to_filepath(directory / blob, blob)

    def download_blob_as_bytes(self, blob: str) -> bytes:
        """
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        try:
            blob_obj = self._construct_blob(blob)
            return blob_obj.download_as_bytes()
        except google.cloud.exceptions.NotFound as e:
            raise BlobNotFoundException(blob)

    def upload_from_bytes(self, string: bytes, blob: str) -> None:
        blob_obj = self._construct_blob(blob)
        blob_obj.upload_from_string(string)

        LOGGER.debug(f"Bytes uploaded to {blob}")

    def last_modified(self, blob: str) -> datetime:
        """
        Returns:
            datetime: Timezone aware datetime
        Raises:
            BlobNotFoundException: The blob was not found in the bucket.
        """
        blob_obj = self._construct_blob(blob)
        try:
            blob_obj.reload()
        except google.cloud.exceptions.NotFound:
            raise BlobNotFoundException(blob)
        return blob_obj.updated

    def crc32c_checksum(self, blob: str) -> int:
        blob_obj = self._construct_blob(blob)
        try:
            blob_obj.reload()
        except google.cloud.exceptions.NotFound:
            raise BlobNotFoundException(blob)

        # Google encodes their checksums using base64 big-endian byte order. This decodes it.
        # Credit https://stackoverflow.com/a/37395200/15175103
        return struct.unpack(">I", base64.b64decode(blob_obj.crc32c))[-1]

    def copy_blob(self, source_blob: str, destination_blob: str) -> None:
        bucket = self._get_bucket()
        bucket.copy_blob(
            blob=self._construct_blob(blob_path=source_blob),
            destination_bucket=bucket,
            new_name=destination_blob,
        )
