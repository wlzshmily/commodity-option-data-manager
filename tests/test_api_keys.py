import sqlite3

from option_data_manager.api_keys import ApiKeyRepository


def test_api_key_secret_is_only_stored_as_hash() -> None:
    connection = sqlite3.connect(":memory:")
    repository = ApiKeyRepository(connection)

    created = repository.create_key(name="monitor", scope="read")

    row = connection.execute("SELECT * FROM api_keys").fetchone()
    assert created.secret.startswith("odm_")
    assert created.secret not in row
    assert repository.verify(created.secret).fingerprint == created.record.fingerprint


def test_revoked_key_no_longer_verifies() -> None:
    connection = sqlite3.connect(":memory:")
    repository = ApiKeyRepository(connection)
    created = repository.create_key(name="monitor")

    repository.revoke(created.record.key_id)

    assert repository.verify(created.secret) is None

