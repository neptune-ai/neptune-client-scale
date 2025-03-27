from __future__ import annotations

__all__ = (
    "verify_type",
    "verify_non_empty",
    "verify_max_length",
    "verify_value_between",
    "verify_project_qualified_name",
)

from typing import (
    Any,
    Union,
)


def get_type_name(var_type: Union[type, tuple]) -> str:
    if isinstance(var_type, tuple):
        return " or ".join(get_type_name(t) for t in var_type)
    elif hasattr(var_type, "__name__"):
        return var_type.__name__
    else:
        return str(var_type)


def verify_type(var_name: str, var: Any, expected_type: Union[type, tuple]) -> None:
    if not isinstance(var, expected_type):
        raise TypeError(f"{var_name} must be a {get_type_name(expected_type)} (was {type(var)})")


def verify_non_empty(var_name: str, var: Any) -> None:
    if not var:
        raise ValueError(f"{var_name} must not be empty")


def verify_max_length(var_name: str, var: str, max_length: int) -> None:
    byte_len = len(var.encode("utf8"))
    if byte_len > max_length:
        raise ValueError(f"{var_name} must not exceed {max_length} bytes, got {byte_len} bytes.")


def verify_project_qualified_name(var_name: str, var: Any) -> None:
    verify_type(var_name, var, str)
    verify_non_empty(var_name, var)

    project_parts = var.split("/")
    if len(project_parts) != 2:
        raise ValueError(f"{var_name} is not in expected format, should be 'workspace-name/project-name")


def verify_value_between(
    var_name: str, var: Union[int, float], expected_min: Union[int, float], expected_max: Union[int, float]
) -> None:
    if var > expected_max or var < expected_min:
        raise ValueError(f"{var_name} must have a value between {expected_min} and {expected_max}")
