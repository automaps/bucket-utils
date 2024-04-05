__all__ = ["BlobNotFoundException"]


class BlobNotFoundException(Exception):
    def __init__(self, blob_name: str):
        self.message = f"Blob {blob_name} was not found in bucket"
        super().__init__(self.message)
