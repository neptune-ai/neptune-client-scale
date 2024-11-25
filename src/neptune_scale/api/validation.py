from __future__ import annotations

__all__ = (
    "verify_type",
    "verify_non_empty",
    "verify_max_length",
    "verify_project_qualified_name",
    "verify_collection_type",
)

from typing import (
    Any,
    Type,
    Union,
)


def get_type_name(var_type: Union[Type, tuple]) -> str:
    return var_type.__name__ if hasattr(var_type, "__name__") else str(var_type)


def verify_type(var_name: str, var: Any, expected_type: Union[Type, tuple]) -> None:
    try:
        if isinstance(expected_type, tuple):
            type_name = " or ".join(get_type_name(t) for t in expected_type)
        else:
            type_name = get_type_name(expected_type)
    except Exception as e:
        # Just to be sure that nothing weird will be raised here
        raise TypeError(f"Incorrect type of {var_name}") from e

    if not isinstance(var, expected_type):
        raise TypeError(f"{var_name} must be a {type_name} (was {type(var)})")


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


def verify_collection_type(var_name: str, var: Union[list, set, tuple], expected_type: Union[type, tuple]) -> None:
    verify_type(var_name, var, (list, set, tuple))

    for value in var:
        verify_type(f"elements of collection '{var_name}'", value, expected_type)
