import filecmp
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import crc32c
import pytest
import pytz

from bucket_utils import LocalBucket, BlobNotFoundException


def assert_filenames_exist_in_directory(filenames: list[str], directory: Path):
    for filename in filenames:
        assert (directory / filename).exists()


FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def a_blob() -> str:
    return "testfile1.txt"


@pytest.fixture()
def bucket(tmp_path, all_files) -> LocalBucket:
    local_bucket = LocalBucket(bucket_name="test_bucket", directory=tmp_path / "bucket")
    for filepath in all_files:
        local_bucket.upload_filepath(filepath=filepath, blob=filepath.name)

    return local_bucket


@pytest.fixture()
def file_to_upload(tmp_path) -> Path:
    file = tmp_path / "my_upload_file.bin"
    file.write_bytes(b"bla bla just some binary data")
    return file


@pytest.fixture()
def result_path(tmp_path) -> Path:
    return tmp_path / "result"


@pytest.fixture()
def all_files() -> list[Path]:
    filenames = ["testfile1.txt", "testfile2.txt"]
    return [FIXTURE_DIR / filename for filename in filenames]


@pytest.fixture()
def all_blobs(all_files) -> list[str]:
    return [file.name for file in all_files]


@pytest.fixture()
def download_directory(tmp_path) -> Path:
    dir = tmp_path / "download_dir"
    dir.mkdir(exist_ok=True)
    return dir


def test_upload_download_blob_to_bucket(bucket: LocalBucket, file_to_upload: Path, result_path: Path):
    blob_name = "my_upload_file.bin"
    bucket.upload_filepath(file_to_upload, blob_name)
    bucket.download_blob_to_filepath(result_path, blob_name)

    assert filecmp.cmp(file_to_upload, result_path)


class BucketTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bucket = LocalBucket("mapspeople_test_bucket")

    def tearDown(self) -> None:
        self.bucket.delete_all()

    def test_should_upload_to_bucket_with_session_url(self):
        upload_session = self.bucket.create_resumable_upload_session("my_blob")
        upload_session.upload(b"Some fancy data")

        self.assertTrue(self.bucket.exists("my_blob"))
        self.assertEqual(
            "Some fancy data", self.bucket.download_blob_as_text("my_blob")
        )

    def test_should_upload_string_to_bucket(self):
        string = "This is my test string"

        blob = "upload_string.txt"

        self.bucket.upload_from_string(string, blob)

        self.assertTrue(self.bucket.exists(blob))
        self.assertEqual(string, self.bucket.download_blob_as_text(blob))

    def test_can_match_blob_names_beginning_with_a_prefix(self):
        self.bucket.upload_from_string(
            string="just a string", blob="Lorem_ipsum_dollars"
        )

        blob_names = self.bucket.iter_blob_names(prefix="Lorem")

        self.assertEqual(["Lorem_ipsum_dollars"], blob_names)

    def test_can_match_blobs_names_with_forward_slashed_beginning_with_a_prefix(self):
        self.bucket.upload_from_string(
            string="just a string", blob="Lorem/ipsum_dollars"
        )

        blob_names = self.bucket.iter_blob_names(prefix="Lorem")

        self.assertEqual(["ipsum_dollars"], blob_names)

    def test_can_list_blob_paths_with_forward_slashed_beginning_with_a_prefix(self):
        self.bucket.upload_from_string(
            string="just a string", blob="Lorem/ipsum_dollars"
        )

        blob_paths = self.bucket.iter_blob_paths(prefix="Lorem")

        self.assertEqual([str(Path("Lorem") / "ipsum_dollars")], blob_paths)

    def test_can_list_blob_path_where_prefix_is_prefix_of_blob_directory(self):
        self.bucket.upload_from_string(
            string="just a string", blob="Lorem/ipsum_dollars"
        )

        blob_paths = self.bucket.iter_blob_paths(prefix="Lo")

        self.assertEqual([str(Path("Lorem") / "ipsum_dollars")], blob_paths)


def test_should_download_all_blobs(all_blobs, bucket, download_directory):
    bucket.download_blobs_to_directory(all_blobs, download_directory)

    assert_filenames_exist_in_directory(
        filenames=all_blobs, directory=download_directory
    )


@pytest.fixture()
def bucket_with_files_last_modified_in_2000(bucket) -> LocalBucket:
    """
    Creates a local bucket fixture ...
    """
    y2k = datetime(2000, 1, 1, tzinfo=pytz.UTC)
    bucket.last_modified = Mock(return_value=y2k)

    return bucket


def test_should_list_blob_paths_since_date(
    bucket_with_files_last_modified_in_2000, download_directory
):
    nineteen_ninety = datetime(1990, 1, 1, tzinfo=pytz.UTC)
    blobs = bucket_with_files_last_modified_in_2000.iter_blob_paths(
        since_last_modified=nineteen_ninety
    )

    # should not be empty
    assert blobs


def test_should_return_no_blob_paths_when_no_files_where_modified_since_date(
    bucket_with_files_last_modified_in_2000, download_directory: Path
):
    twenty_twenty = datetime(2020, 1, 1, tzinfo=pytz.UTC)
    blobs = bucket_with_files_last_modified_in_2000.iter_blob_paths(
        since_last_modified=twenty_twenty
    )

    assert blobs == []


def test_should_list_blob_names_since_date(
    bucket_with_files_last_modified_in_2000, download_directory
):
    nineteen_ninety = datetime(1990, 1, 1, tzinfo=pytz.UTC)
    blobs = bucket_with_files_last_modified_in_2000.iter_blob_names(
        since_last_modified=nineteen_ninety
    )

    # should not be empty
    assert blobs


def test_should_return_no_blob_names_when_no_files_where_modified_since_date(
    bucket_with_files_last_modified_in_2000, download_directory: Path
):
    twenty_twenty = datetime(2020, 1, 1, tzinfo=pytz.UTC)
    blobs = bucket_with_files_last_modified_in_2000.iter_blob_paths(
        since_last_modified=twenty_twenty
    )

    assert blobs == []


def test_last_modified_is_timezone_aware(bucket, a_blob):
    date = bucket.last_modified(a_blob)

    assert date.tzinfo is not None


def test_should_return_crc32c_checksum(bucket, a_blob):
    checksum = bucket.crc32c_checksum(a_blob)
    assert checksum
    assert isinstance(checksum, int)


def test_checksums_should_be_equal(bucket):
    original_checksum = crc32c.crc32c(b"Lorem ipsum")
    bucket.upload_from_string("Lorem ipsum", blob="li.txt")
    assert original_checksum == bucket.crc32c_checksum("li.txt")


def test_checksum_fails_if_blob_does_not_exist(bucket):
    with pytest.raises(BlobNotFoundException):
        bucket.crc32c_checksum("blob_that_does_not_exist.txt")
