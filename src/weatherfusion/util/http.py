from __future__ import annotations

from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_TIMEOUT = 30


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, timeout: int = DEFAULT_TIMEOUT, **kwargs) -> None:
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self.timeout)
        return super().send(request, **kwargs)


def create_session(user_agent: str, retries: int = 3, backoff: float = 0.3, status_forcelist: Iterable[int] = (500, 502, 503, 504)) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff,
        status_forcelist=status_forcelist,
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = TimeoutHTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
