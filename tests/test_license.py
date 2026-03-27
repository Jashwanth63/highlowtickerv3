import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import base64
import json

import pytest

import core.license as lic


@pytest.fixture(autouse=True)
def isolated_config_path(monkeypatch):
    config_path = Path(__file__).resolve().parent / "_test_tmp_license.toml"
    if config_path.exists():
        config_path.unlink()
    monkeypatch.setattr(lic, "CONFIG_PATH", config_path)
    yield
    if config_path.exists():
        config_path.unlink()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _make_signed_key(monkeypatch, payload: dict) -> str:
    fake_public_key = type(
        "FakePublicKey",
        (),
        {"verify": lambda self, sig, data, padding, algorithm: None},
    )()
    monkeypatch.setattr(lic.serialization, "load_pem_public_key", lambda pem: fake_public_key)
    payload_b64 = _b64url(json.dumps(payload).encode())
    sig_b64 = _b64url(b"sig")
    return f"v{payload.get('ver', 1)}.{payload_b64}.{sig_b64}"


def test_validate_no_key_returns_invalid():
    result = lic.validate()
    assert result.valid is False
    assert result.version == ""
    assert result.machine_bound is False
    assert result.machine_match is False
    assert result.message == ""


def test_validate_invalid_format_returns_message():
    result = lic.validate("not-a-license")
    assert result.valid is False
    assert "Invalid key format" in result.message


def test_validate_invalid_signature_returns_message():
    result = lic.validate("v1.payload.signature")
    assert result.valid is False
    assert "invalid signature" in result.message.lower()


def test_validate_unbound_key_is_valid(monkeypatch):
    key = _make_signed_key(monkeypatch, {"ver": 1, "uid": "u1", "mid": ""})
    result = lic.validate(key)
    assert result.valid is True
    assert result.version == "1"
    assert result.machine_bound is False
    assert result.machine_match is True
    assert result.message == ""


def test_validate_machine_bound_key_matches(monkeypatch):
    monkeypatch.setattr(lic, "machine_id", lambda: "machine-123")
    key = _make_signed_key(monkeypatch, {"ver": 2, "uid": "u1", "mid": "machine-123"})
    result = lic.validate(key)
    assert result.valid is True
    assert result.version == "2"
    assert result.machine_bound is True
    assert result.machine_match is True
    assert result.message == ""


def test_validate_machine_bound_key_mismatch_warns(monkeypatch):
    monkeypatch.setattr(lic, "machine_id", lambda: "machine-abc")
    key = _make_signed_key(monkeypatch, {"ver": 2, "uid": "u1", "mid": "different-machine"})
    result = lic.validate(key)
    assert result.valid is True
    assert result.machine_bound is True
    assert result.machine_match is False
    assert "different machine" in result.message.lower()


def test_get_license_key_returns_none_when_no_config():
    assert lic.get_license_key() is None


def test_get_license_key_returns_none_when_key_missing():
    lic.CONFIG_PATH.write_text("[license]\nother = \"field\"\n")
    assert lic.get_license_key() is None


def test_get_license_key_returns_key_when_present():
    lic.CONFIG_PATH.write_text("[license]\nkey = \"my-license-key\"\n")
    assert lic.get_license_key() == "my-license-key"


def test_save_license_key_creates_license_section():
    lic.save_license_key("saved-key")
    text = lic.CONFIG_PATH.read_text()
    assert "[license]" in text
    assert 'key = "saved-key"' in text


def test_save_license_key_updates_existing_key():
    lic.CONFIG_PATH.write_text("[license]\nkey = \"old-key\"\n")
    lic.save_license_key("new-key")
    text = lic.CONFIG_PATH.read_text()
    assert 'key = "new-key"' in text
    assert 'key = "old-key"' not in text
