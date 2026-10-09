from dataclasses import dataclass
import re


@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"
    schema: str = "ivanrazumovskyi_lab7"
    bronze_schema: str = "ivanrazumovskyi_lab5"
    gold_schema: str = "ivanrazumovskyi_lab6"

    def __post_init__(self):
        for value in (self.catalog, self.schema, self.bronze_schema, self.gold_schema):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                raise ValueError("Unsafe catalog/schema identifier")
        if self.schema in {self.bronze_schema, self.gold_schema} or not self.schema.endswith(
            "_lab7"
        ):
            raise ValueError("Lab 7 outputs must use a separate schema ending in _lab7")

    @property
    def namespace(self):
        return f"{self.catalog}.{self.schema}"

    def table(self, name):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Unsafe table identifier")
        return f"{self.namespace}.{name}"

    def source(self, name, gold=False):
        schema = self.gold_schema if gold else self.bronze_schema
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Unsafe source identifier")
        return f"{self.catalog}.{schema}.{name}"


def notebook_config(widgets):
    """Register common parameters and validate their values without an SDK client."""
    defaults = Config()
    names = ("catalog", "schema", "bronze_schema", "gold_schema")
    for name in names:
        widgets.text(name, getattr(defaults, name))
    return Config(**{name: widgets.get(name) for name in names})
