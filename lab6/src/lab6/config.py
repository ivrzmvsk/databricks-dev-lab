from dataclasses import dataclass
import re


@dataclass
class Config:
    catalog: str = "workspace"
    schema: str = "ivanrazumovskyi_lab6"
    source_schema: str = "ivanrazumovskyi_lab5"
    metadata_path: str = ""
    refresh_reference: bool = False
    drop_threshold_pct: float = 50.0

    def __post_init__(self):
        for identifier in (self.catalog, self.schema, self.source_schema):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
                raise ValueError("Invalid catalog/schema identifier")
        if self.schema == self.source_schema:
            raise ValueError("Gold must use a separate schema")
        if not 0 < self.drop_threshold_pct < 100:
            raise ValueError("drop_threshold_pct must be between 0 and 100")
        if not self.metadata_path:
            self.metadata_path = f"/Volumes/{self.catalog}/{self.schema}/lab6_assets/metadata/wikimedia.json"

    @property
    def namespace(self):
        return f"{self.catalog}.{self.schema}"

    @property
    def source(self):
        return f"{self.catalog}.{self.source_schema}.wiki_silver_lab5"

    def table(self, name):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Invalid table identifier")
        return f"{self.namespace}.{name}"


def from_widgets(dbutils):
    defaults = {"catalog": "workspace", "schema": "ivanrazumovskyi_lab6",
                "source_schema": "ivanrazumovskyi_lab5", "metadata_path": "",
                "refresh_reference": "false", "drop_threshold_pct": "50"}
    for name, default in defaults.items():
        dbutils.widgets.text(name, default)
    values = {name: dbutils.widgets.get(name) for name in defaults}
    values["refresh_reference"] = values["refresh_reference"].lower() == "true"
    values["drop_threshold_pct"] = float(values["drop_threshold_pct"])
    return Config(**values)
