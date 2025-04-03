import pytest

from neptune_scale.util.envs import get_option


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
