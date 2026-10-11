import pytest

from lab8.contracts import bootstrap_mode, check_delivery_counts
from lab8.migrations import pending_migrations


def test_first_and_interrupted_build_recovery():
    expected = ['fact', 'dimension']
    assert bootstrap_mode([], expected)
    assert bootstrap_mode(['dimension'], expected)
    assert bootstrap_mode(expected, expected)
    assert not bootstrap_mode(expected, expected, True)
    with pytest.raises(ValueError, match='missing'):
        bootstrap_mode(['fact'], expected, True)


@pytest.mark.parametrize('counts', [(10, 8, 2, 9), (10, 8, 1, 10), (10, 0, 10, 10)])
def test_delivery_loss_duplicates_or_empty_source_fail(counts):
    with pytest.raises(ValueError):
        check_delivery_counts(*counts)


def test_complete_delivery_partition():
    check_delivery_counts(10, 8, 2, 10)


def test_migrations_are_repeatable_and_append_only(tmp_path):
    first = tmp_path / '001.sql'
    first.write_text('CREATE TABLE IF NOT EXISTS test (id STRING)')
    planned = pending_migrations([first], {})
    applied = {planned[0][0]: planned[0][1]}
    assert pending_migrations([first], applied) == []
    second = tmp_path / '002.sql'
    second.write_text('ALTER TABLE test ADD COLUMN name STRING')
    assert [p[0] for p in pending_migrations([first, second], applied)] == ['002.sql']
    first.write_text('DROP TABLE test')
    with pytest.raises(ValueError, match='changed'):
        pending_migrations([first, second], applied)
    with pytest.raises(ValueError, match='removed'):
        pending_migrations([second], applied)
