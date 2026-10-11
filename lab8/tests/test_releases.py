from types import SimpleNamespace

import pytest

from lab8.releases import (
    check_repeatability, gold_build_fingerprint, validate_build_fingerprint,
)


def test_build_fingerprint_requires_a_lowercase_sha256():
    validate_build_fingerprint('0123456789abcdef' * 4)
    for invalid in ('', 'a' * 63, 'a' * 65, 'g' * 64, 'A' * 64):
        with pytest.raises(ValueError, match='valid build fingerprint'):
            validate_build_fingerprint(invalid)


def test_repeatability_is_scoped_to_data_and_build_inputs():
    previous = SimpleNamespace(source_version=3, build_fingerprint='build-a',
                               fact_count=20, fact_checksum=123)
    check_repeatability([previous], 3, 'build-a', 20, 123)
    check_repeatability([previous], 3, 'build-b', 21, 456)
    check_repeatability([previous], 4, 'build-a', 21, 456)
    with pytest.raises(ValueError, match='same Silver version and build inputs'):
        check_repeatability([previous], 3, 'build-a', 20, 456)
    with pytest.raises(ValueError):
        check_repeatability([previous], 3, 'build-a', 21, 123)
    previous.build_fingerprint = None  # Old audit rows remain valid history.
    check_repeatability([previous], 3, 'build-a', 21, 456)


def test_build_fingerprint_tracks_code_reference_and_policy(tmp_path):
    paths = ['lab8/src/lab8/gold.py', 'lab8/assets/wikimedia.json',
             'lab8/assets/access_policy.json', 'lab6/src/lab6/transforms.py',
             'lab8/src/lab8/releases.py']
    for name in paths:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    original = gold_build_fingerprint(tmp_path)
    assert gold_build_fingerprint(tmp_path) == original
    for name in paths:
        path = tmp_path / name
        path.write_text(name + 'changed')
        assert gold_build_fingerprint(tmp_path) != original
        path.write_text(name)
    extra = tmp_path / 'lab6/src/lab6/new_helper.py'
    extra.write_text('new helper')
    assert gold_build_fingerprint(tmp_path) != original
    extra.unlink()
    assert gold_build_fingerprint(tmp_path) == original
