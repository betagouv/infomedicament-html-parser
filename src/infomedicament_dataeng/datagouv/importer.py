"""Fetch and import open data from data.gouv.fr."""

import csv
import io
import logging
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import yaml
from sqlalchemy import text

from ..config import PostgresConfig, get_config
from ..db import get_postgres_engine

logger = logging.getLogger(__name__)

BASE_URL = "https://www.data.gouv.fr/api/1/datasets/r/"

# Map YAML type strings to Python SQL types (extensible for future types)
_SQL_TYPES = {"str": "text"}


@dataclass
class ColumnDef:
    name: str
    type: str  # YAML type string, e.g. "str"


@dataclass
class CsvSource:
    delimiter: str
    encoding: str
    quotechar: str = '"'
    has_header: bool = True


@dataclass
class DataGouvDataset:
    datagouv_dataset_id: str
    postgresql_table: str
    source: CsvSource
    columns: list[ColumnDef]


def load_datasets(config_path: Path) -> dict[str, DataGouvDataset]:
    """Load dataset descriptors from a YAML config file."""
    with config_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    datasets = {}
    for name, d in raw["datasets"].items():
        src = d["source"]
        if src["type"] != "csv":
            raise ValueError(f"Unsupported source type {src['type']!r} for dataset {name!r}")
        datasets[name] = DataGouvDataset(
            datagouv_dataset_id=d["datagouv_dataset_id"],
            postgresql_table=d["postgresql_table"],
            source=CsvSource(
                delimiter=src["delimiter"],
                encoding=src["encoding"],
                quotechar=src.get("quotechar", '"'),
                has_header=src.get("has_header", True),
            ),
            columns=[ColumnDef(name=c["name"], type=c["type"]) for c in d["columns"]],
        )
    return datasets


def fetch_csv(dataset: DataGouvDataset) -> list[list[str]]:
    """Fetch the dataset CSV from data.gouv.fr, returning data rows.

    The header row is skipped unless ``source.has_header`` is False.
    """
    url = BASE_URL + dataset.datagouv_dataset_id
    with urllib.request.urlopen(url) as response:
        content = response.read().decode(dataset.source.encoding)
    reader = csv.reader(io.StringIO(content), delimiter=dataset.source.delimiter, quotechar=dataset.source.quotechar)
    rows = list(reader)
    return rows[1:] if dataset.source.has_header else rows


def import_dataset(dataset: DataGouvDataset, config: PostgresConfig | None = None) -> int:
    """Truncate the target table and insert all rows fetched from data.gouv.fr.

    Returns:
        Number of rows imported.
    """
    if config is None:
        config = get_config().postgres

    rows = fetch_csv(dataset)
    expected_cols = len(dataset.columns)
    valid_rows = [row for row in rows if len(row) == expected_cols]
    if len(valid_rows) < len(rows):
        logger.warning(
            f"Skipped {len(rows) - len(valid_rows)} rows with unexpected column count (expected {expected_cols})"
        )
    rows = valid_rows
    logger.info(f"Fetched {len(rows)} rows for table '{dataset.postgresql_table}'")

    col_names = ", ".join(c.name for c in dataset.columns)

    # Serialize rows to CSV with every field quoted, so empty fields round-trip as
    # empty strings (not NULL) and delimiters/quotes/newlines are escaped. All cells
    # are already strings (from csv.reader), so no type/null handling is needed.
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    writer.writerows(rows)
    buf.seek(0)

    engine = get_postgres_engine(config)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {dataset.postgresql_table}"))
        if rows:
            raw = conn.connection.dbapi_connection
            with raw.cursor() as cur:
                cur.copy_expert(
                    f"COPY {dataset.postgresql_table} ({col_names}) FROM STDIN WITH (FORMAT csv)",
                    buf,
                )
    logger.info(f"Imported {len(rows)} rows into '{dataset.postgresql_table}'")
    return len(rows)
