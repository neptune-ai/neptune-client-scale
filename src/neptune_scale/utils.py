from datetime import datetime
from typing import Any


def stringify_unsupported(d: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    """
    A helper function that flattens a nested dictionary structure and casts unsupported values to strings to be logged in Neptune.
    Note:
    - Collections (list, set, tuple) are converted to strings.
    - None values are ignored.

    Args:
        d: Dictionary to flatten

    Returns:
        dict: Flattened dictionary with string keys and cast values

    Example:
        >>> config = {"mixed_nested": {"list": [1, {"a": 2}, 3, None], "dict": {"a": [1, 2], "b": {"c": 3}, "d": None}}}
        >>> run.log_configs(config)  # without `stringify_unsupported()`
        neptune:WARNING: Dropping value. Config values must be float, bool, int, str, datetime, list, set or tuple (got `mixed_nested`:`{'list': [1, {'a': 2}, 3, None], 'dict': {'a': [1, 2], 'b': {'c': 3}}}`)
        >>> run.log_configs(stringify_unsupported(config))  # with `stringify_unsupported()`
        Logged successfully as
        "mixed_nested": {
            "list": "[1, {'a': 2}, 3, None]",
            "dict": {
                "a": "[1, 2]",
                "b": {"c": 3},
            }
        }


    For more details, see https://docs.neptune.ai/api/utils/#stringify_unsupported
    """
    if not isinstance(d, dict):
        raise TypeError("Input must be a dictionary")

    allowed_datatypes = [int, float, str, datetime, bool, list, set, tuple]

    flattened = {}

    def _stringify_unsupported(d: dict[str, Any], prefix: str = "") -> None:
        for key, value in d.items():
            new_key = f"{prefix}/{key}" if prefix else f"{prefix}{key}"
            if isinstance(value, dict):
                _stringify_unsupported(d=value, prefix=new_key)
            elif isinstance(value, (list, set, tuple)):
                flattened[new_key] = str(value)
            elif type(value) in allowed_datatypes:
                flattened[new_key] = value
            elif value is not None:
                flattened[new_key] = str(value)

    _stringify_unsupported(d)
    return flattened
