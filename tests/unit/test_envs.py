import pytest

from neptune_scale.util.envs import (
    ALLOW_SELF_SIGNED_CERTIFICATE,
    VERIFY_SSL,
    get_bool,
    get_option,
)


@pytest.mark.parametrize(
    "env_value, choices, default, expected",
    (
        (None, ("foo", "bar"), "foo", "foo"),
        ("bar", ("foo", "bar"), "foo", "bar"),
        ("BAR", ("foo", "bar"), "foo", "bar"),
    ),
)
def test_get_option_known(monkeypatch, env_value, choices, default, expected):
    if env_value is not None:
        monkeypatch.setenv("VAR_NAME", env_value)

    result = get_option("VAR_NAME", choices, default)
    assert result == expected


def test_get_option_unknown(monkeypatch):
    monkeypatch.setenv("VAR_NAME", "unknown")

    with pytest.raises(ValueError) as exc:
        get_option("VAR_NAME", ("foo", "bar"), "foo")

    exc.match("VAR_NAME must be one of: foo, bar, got 'unknown'")


@pytest.mark.parametrize(
    "verify_ssl, allow_self_signed_certificate, expected",
    (
        (None, None, True),
        (None, "false", True),
        (None, "true", False),
        (None, "xyz", True),
        ("false", None, False),
        ("false", "false", False),
        ("false", "true", False),
        ("false", "xyz", False),
        ("true", None, True),
        ("true", "false", True),
        ("true", "true", True),
        ("true", "xyz", True),
        ("xyz", None, True),
        ("xyz", "false", True),
        ("xyz", "true", True),
        ("xyz", "xyz", True),
    ),
)
def test_ssl_option_fallback(monkeypatch, verify_ssl, allow_self_signed_certificate, expected):
    if verify_ssl is not None:
        monkeypatch.setenv(VERIFY_SSL, verify_ssl)
    else:
        monkeypatch.delenv(VERIFY_SSL, raising=False)
    if allow_self_signed_certificate is not None:
        monkeypatch.setenv(ALLOW_SELF_SIGNED_CERTIFICATE, allow_self_signed_certificate)
    else:
        monkeypatch.delenv(ALLOW_SELF_SIGNED_CERTIFICATE, raising=False)

    result = get_bool(
        VERIFY_SSL,
        default_invalid=True,
        default_missing=not get_bool(ALLOW_SELF_SIGNED_CERTIFICATE, default_missing=False, default_invalid=False),
    )

    assert result == expected
