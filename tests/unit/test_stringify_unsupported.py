from datetime import datetime
from typing import Any

import pytest

from neptune_scale.utils import stringify_unsupported


def test_stringify_unsupported_basic_types():
    """Test stringify_unsupported with basic Python types."""
    # Test with basic types
    test_cases: dict[str, Any] = {
        "string": "hello",
        "integer": 42,
        "float": 3.14,
        "boolean": True,
        "none_value": None,
        "datetime": datetime.now(),
    }

    result = stringify_unsupported(test_cases)

    assert result["string"] == "hello"
    assert result["integer"] == 42
    assert result["float"] == 3.14
    assert result["boolean"] is True
    assert "none_value" not in result
    assert isinstance(result["datetime"], datetime)


def test_stringify_unsupported_collections():
    """Test stringify_unsupported with collection types."""
    test_cases = {
        "list": ["a", "b", "c"],
        "tuple": (1, 2, 3),
        "set": {1, 2, 3},
        "frozenset": frozenset({1, 2, 3}),
    }

    result = stringify_unsupported(test_cases)

    assert result["list"] == "['a', 'b', 'c']"
    assert result["tuple"] == "(1, 2, 3)"
    assert result["set"] == "{1, 2, 3}"
    assert result["frozenset"] == "frozenset({1, 2, 3})"


def test_stringify_unsupported_nested():
    """Test stringify_unsupported with nested dictionaries."""
    test_cases = {"top": {"middle": {"bottom": "value"}}}

    result = stringify_unsupported(test_cases)

    assert result["top/middle/bottom"] == "value"


def test_stringify_unsupported_mixed_types():
    """Test stringify_unsupported with mixed types in collections."""
    test_cases = {
        "mixed_list": [1, "two", 3.0, True, None],
        "mixed_dict": {"str": "value", "int": 123, "list": [1, 2, 3], "none": None},
        "mixed_set": {1, "two", 3.0, True},
        "mixed_tuple": (1, "two", 3.0, True, None),
    }

    result = stringify_unsupported(test_cases)

    assert result["mixed_list"] == "[1, 'two', 3.0, True, None]"
    assert result["mixed_dict/str"] == "value"
    assert result["mixed_dict/int"] == 123
    assert result["mixed_dict/list"] == "[1, 2, 3]"
    assert "mixed_dict/none" not in result
    assert result["mixed_set"] == str(test_cases["mixed_set"])
    assert result["mixed_tuple"] == str(test_cases["mixed_tuple"])


def test_stringify_unsupported_edge_cases():
    """Test stringify_unsupported with edge cases."""
    test_cases = {
        "empty_list": [],
        "empty_dict": {},
        "empty_string": "",
        "zero": 0,
        "false": False,
        "empty_set": set(),
        "empty_tuple": (),
    }

    result = stringify_unsupported(test_cases)

    assert result["empty_list"] == "[]"
    assert "empty_dict" not in result
    assert result["empty_string"] == ""
    assert result["zero"] == 0
    assert result["false"] is False
    assert result["empty_set"] == "set()"
    assert result["empty_tuple"] == "()"


def test_stringify_unsupported_complex_nested():
    """Test stringify_unsupported with complex nested structures."""
    test_cases = {
        "complex": {
            "list_of_dicts": [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}],
            "mixed_nested": {
                "list": [1, {"a": 2}, 3, None],
                "dict": {"a": [1, 2], "b": {"c": 3}},
                "set": {1, 2, 3},
                "tuple": (1, 2, 3),
            },
        }
    }

    result = stringify_unsupported(test_cases)

    assert result["complex/list_of_dicts"] == "[{'id': 1, 'name': 'one'}, {'id': 2, 'name': 'two'}]"
    assert result["complex/mixed_nested/list"] == "[1, {'a': 2}, 3, None]"
    assert result["complex/mixed_nested/dict/a"] == "[1, 2]"
    assert result["complex/mixed_nested/dict/b/c"] == 3
    assert result["complex/mixed_nested/set"] == "{1, 2, 3}"
    assert result["complex/mixed_nested/tuple"] == "(1, 2, 3)"


def test_stringify_unsupported_custom_objects():
    """Test stringify_unsupported with custom objects."""

    class CustomObject:
        def __str__(self):
            return "custom_object"

    test_cases = {"custom": CustomObject()}

    result = stringify_unsupported(test_cases)

    assert result["custom"] == "custom_object"


def test_stringify_unsupported_none_values():
    """Test stringify_unsupported with None values in collections."""
    test_cases = {
        "list_with_none": [1, None, 3],
        "dict_with_none": {"a": 1, "b": None, "c": 3},
        "set_with_none": {1, None, 3},
        "tuple_with_none": (1, None, 3),
    }

    result = stringify_unsupported(test_cases)

    assert result["list_with_none"] == "[1, None, 3]"
    assert result["dict_with_none/a"] == 1
    assert "dict_with_none/b" not in result
    assert result["dict_with_none/c"] == 3
    assert result["set_with_none"] == "{None, 1, 3}"
    assert result["tuple_with_none"] == "(1, None, 3)"


def test_stringify_unsupported_empty_input():
    """Test stringify_unsupported with empty input."""
    result = stringify_unsupported({})
    assert result == {}


def test_stringify_unsupported_invalid_input():
    """Test stringify_unsupported with invalid input."""
    with pytest.raises(TypeError):
        stringify_unsupported(None)  # type: ignore

    with pytest.raises(TypeError):
        stringify_unsupported("not a dict")  # type: ignore
