"""FastAPI skeleton for application settings."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from option_data_manager.settings import (
    SettingsRepository,
    TQSDK_ACCOUNT_KEY,
    TQSDK_PASSWORD_KEY,
)


class TqsdkCredentialUpdate(BaseModel):
    """Request body for saving TQSDK credentials."""

    account: str
    password: str


class TqsdkCredentialStatus(BaseModel):
    """Credential status that is safe to return to API consumers."""

    account: str | None
    password_configured: bool


def create_settings_app(repository: SettingsRepository) -> FastAPI:
    """Create a local settings API bound to a settings repository."""

    app = FastAPI(title="Option Data Manager API")

    @app.get("/api/settings/tqsdk", response_model=TqsdkCredentialStatus)
    def get_tqsdk_credentials() -> TqsdkCredentialStatus:
        return _credential_status(repository)

    @app.put("/api/settings/tqsdk", response_model=TqsdkCredentialStatus)
    @app.put("/api/settings/tqsdk-credentials", response_model=TqsdkCredentialStatus)
    def save_tqsdk_credentials(
        payload: TqsdkCredentialUpdate,
    ) -> TqsdkCredentialStatus:
        account = payload.account.strip()
        if not account:
            raise HTTPException(
                status_code=400,
                detail="TQSDK account must not be empty.",
            )
        if not payload.password:
            raise HTTPException(
                status_code=400,
                detail="TQSDK password must not be empty.",
            )

        repository.set_value(TQSDK_ACCOUNT_KEY, account)
        repository.set_secret(TQSDK_PASSWORD_KEY, payload.password)
        return _credential_status(repository)

    return app


def _credential_status(repository: SettingsRepository) -> TqsdkCredentialStatus:
    account = repository.get_value(TQSDK_ACCOUNT_KEY)
    password_configured = repository.get_secret(TQSDK_PASSWORD_KEY) is not None
    return TqsdkCredentialStatus(
        account=account,
        password_configured=password_configured,
    )
