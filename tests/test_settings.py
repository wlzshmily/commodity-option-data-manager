from option_data_manager.settings import FernetFileProtector


def test_fernet_file_protector_round_trip(tmp_path) -> None:
    key_path = tmp_path / "secret.key"
    protector = FernetFileProtector(key_path)

    protected = protector.protect("sensitive-value")

    assert protected.startswith("fernet:")
    assert "sensitive-value" not in protected
    assert protector.unprotect(protected) == "sensitive-value"
    assert key_path.exists()


def test_fernet_file_protector_reuses_existing_key(tmp_path) -> None:
    key_path = tmp_path / "secret.key"
    first = FernetFileProtector(key_path)
    protected = first.protect("sensitive-value")

    second = FernetFileProtector(key_path)

    assert second.unprotect(protected) == "sensitive-value"
