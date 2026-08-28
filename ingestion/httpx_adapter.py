"""Concrete httpx transport adapter with bounded retry behavior."""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpResponse, RequestContext, Sleeper
from application.retry import RetryPolicy


@dataclass(frozen=True, slots=True)
class TimeoutConfig:
    """Explicit connect/read/write/pool timeout contract."""

    connect: float = 5.0
    read: float = 30.0
    write: float = 30.0
    pool: float = 5.0

    def as_httpx(self) -> httpx.Timeout:
        return httpx.Timeout(
            connect=self.connect,
            read=self.read,
            write=self.write,
            pool=self.pool,
        )


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def _safe_request_path(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    return f"{parsed.scheme}://{host}{parsed.path}"


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value >= 0 else None


class HttpxTransport:
    """Adapter implementing the application HTTP port through httpx."""

    def __init__(
        self,
        *,
        timeout: TimeoutConfig | None = None,
        retry_policy: RetryPolicy | None = None,
        sleeper: Sleeper = _sleep,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._timeout = timeout if timeout is not None else TimeoutConfig()
        self._retry_policy = retry_policy if retry_policy is not None else RetryPolicy()
        self._sleeper = sleeper
        self._client = httpx.Client(timeout=self._timeout.as_httpx(), transport=transport)

    @property
    def timeout(self) -> TimeoutConfig:
        return self._timeout

    def close(self) -> None:
        self._client.close()

    def send(self, request: HttpRequest, *, context: RequestContext) -> HttpResponse:
        safe_path = _safe_request_path(request.url)
        transport_exhausted = False
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                response = self._client.request(
                    request.method,
                    request.url,
                    params=request.params,
                    headers=request.headers,
                )
            except httpx.TransportError:
                if attempt >= self._retry_policy.max_attempts:
                    transport_exhausted = True
                    break
                self._sleeper(self._retry_policy.delay_after(attempt))
                continue

            if self._retry_policy.retryable_status(response.status_code):
                if attempt >= self._retry_policy.max_attempts:
                    raise ProviderHttpError(
                        context=context,
                        category="http_retry_exhausted",
                        request_path=safe_path,
                        status_code=response.status_code,
                    )
                self._sleeper(
                    self._retry_policy.delay_after(
                        attempt,
                        _retry_after_seconds(response),
                    )
                )
                continue

            return HttpResponse(
                status_code=response.status_code,
                content=response.content,
                headers=dict(response.headers),
            )

        if transport_exhausted:
            raise ProviderHttpError(
                context=context,
                category="transport_exhausted",
                request_path=safe_path,
            ) from None
        raise AssertionError("retry loop exhausted without terminal result")
