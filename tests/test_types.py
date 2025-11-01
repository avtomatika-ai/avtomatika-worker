from avtomatika_worker import types


def test_types_constants():
    """Tests that the constants in the types module are defined correctly."""
    assert types.TRANSIENT_ERROR == "TRANSIENT_ERROR"
    assert types.PERMANENT_ERROR == "PERMANENT_ERROR"
    assert types.INVALID_INPUT_ERROR == "INVALID_INPUT_ERROR"
