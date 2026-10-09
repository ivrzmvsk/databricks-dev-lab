import pytest

from lab7.runtime import constraint_error_condition, is_expected_constraint_error


class StructuredDeltaError(Exception):
    def __init__(self, condition):
        super().__init__("Localized error text without any constraint names")
        self.condition = condition

    def getCondition(self):
        return self.condition


@pytest.mark.parametrize("condition,column,constraint,expected", [
    ("DELTA_NOT_NULL_CONSTRAINT_VIOLATED", "event_id", None, True),
    ("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "edit_count", "one_edit", True),
    ("DELTA_VIOLATE_CONSTRAINT_WITHOUT_VALUES", "edit_count", "one_edit", True),
    ("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "old_length", "lengths_nonnegative", True),
    ("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "bytes_delta", "bytes_consistent", True),
    ("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "bytes_delta", "one_edit", False),
    ("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "old_length", None, False),
    ("PERMISSION_DENIED", "edit_count", "one_edit", False),
    ("DELTA_NOT_NULL_CONSTRAINT_VIOLATED", "edit_count", "one_edit", False),
])
def test_constraint_rejections_use_structured_conditions(condition, column, constraint, expected):
    error = StructuredDeltaError(condition)
    assert is_expected_constraint_error(constraint_error_condition(error), column,
                                        {"constraintName": constraint}) is expected


def test_error_text_alone_never_counts_as_constraint_evidence():
    error = Exception("ONE_EDIT CHECK_CONSTRAINT DELTA_NOT_NULL_CONSTRAINT_VIOLATED")
    assert constraint_error_condition(error) is None
    assert not is_expected_constraint_error(None, "edit_count")
