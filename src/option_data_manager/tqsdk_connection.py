"""TQSDK connection helpers with secret-safe retry handling."""

from __future__ import annotations

from collections.abc import Callable
import os
import time
from typing import Any


DEFAULT_CONNECT_ATTEMPTS = 3
DEFAULT_CONNECT_RETRY_DELAY_SECONDS = 5.0


class TqsdkConnectionError(RuntimeError):
    """Raised after TQSDK connection attempts are exhausted."""


def create_tqsdk_api_with_retries(
    account: str,
    password: str,
    *,
    attempts: int | None = None,
    retry_delay_seconds: float | None = None,
    api_factory: Callable[[str, str], Any] | None = None,
) -> Any:
    """Create a TQSDK API object, retrying transient login/network failures."""

    configured_attempts = (
        attempts
        if attempts is not None
        else _env_int("ODM_TQSDK_CONNECT_ATTEMPTS", DEFAULT_CONNECT_ATTEMPTS)
    )
    max_attempts = max(configured_attempts, 1)
    delay = max(
        retry_delay_seconds
        if retry_delay_seconds is not None
        else _env_float("ODM_TQSDK_CONNECT_RETRY_DELAY", DEFAULT_CONNECT_RETRY_DELAY_SECONDS),
        0.0,
    )
    factory = api_factory or _create_tqsdk_api_once
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return factory(account, password)
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts and delay:
                time.sleep(delay)
    if last_error is None:
        raise TqsdkConnectionError("TQSDK connection failed.")
    raise TqsdkConnectionError(
        f"TQSDK connection failed after {max_attempts} attempt(s): "
        f"{type(last_error).__name__}: {last_error}"
    ) from last_error


def _create_tqsdk_api_once(account: str, password: str) -> Any:
    from tqsdk import TqApi, TqAuth

    return TqApi(auth=TqAuth(account, password), web_gui=False, disable_print=True)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default
