import requests

from .interface import ResumableUploadSessionInterface

__all__ = ["HTTPResumableUploadSession"]


class HTTPResumableUploadSession(ResumableUploadSessionInterface):
    def __init__(self, url, *args, **kwargs):
        self._url = url

    def get_url(self):
        return self._url

    def upload(self, data):
        requests.put(url=self.get_url(), data=data)
