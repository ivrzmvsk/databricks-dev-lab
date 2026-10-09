import pytest

from lab7.config import Config, notebook_config


class Widgets:
    """Minimal widget protocol: registering defaults preserves existing Job values."""

    def __init__(self, values):
        self.values = dict(values)

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


def test_notebook_defaults_and_job_overrides():
    assert notebook_config(Widgets({})) == Config()
    assert notebook_config(Widgets({"catalog": "custom", "schema": "review_lab7"})) == Config(
        catalog="custom", schema="review_lab7"
    )


def test_notebook_rejects_source_schema_as_output():
    with pytest.raises(ValueError, match="separate schema"):
        notebook_config(Widgets({"schema": "ivanrazumovskyi_lab6"}))
