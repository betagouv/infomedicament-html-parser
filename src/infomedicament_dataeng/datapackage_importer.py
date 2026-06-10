"""Import the ANSM frictionless datapackage into PostgreSQL."""

import logging
import os
import tempfile
import urllib.request

from frictionless import Package
from sqlalchemy import text

from .config import PostgresConfig, get_config
from .db import get_postgres_engine

logger = logging.getLogger(__name__)

# Respects FK dependency order: parents before children.
LOAD_ORDER = [
    "specialite",
    "atc",
    "classe_clinique",
    "groupe_substance",
    "classe_interaction",
    "presentation",
    "element",
    "specialite_atc",
    "specialite_classe_clinique",
    "classe_groupe_substance",
    "substance_groupe_substance",
    "interaction",
    "composant",
    "recipient",
    "dispositif",
    "document",
    "caracteristique",
    "specialite_titulaire",
]

TABLE_PREFIX = "ansm_"


def import_datapackage(
    package_path: str,
    resource_name: str | None = None,
    config: PostgresConfig | None = None,
) -> dict[str, int]:
    """Load resources from a frictionless datapackage into PostgreSQL.

    Tables must already exist (created by Kysely migrations in the infomedicament repo).
    Each table is truncated then fully reloaded from the CSV.

    Args:
        package_path: Path or URL to a datapackage.json or a zip containing one.
        resource_name: If given, load only this resource; otherwise load all in
                       dependency order.
        config: PostgreSQL connection config (defaults to env).

    Returns:
        Dict mapping table name to number of rows inserted.
    """
    if config is None:
        config = get_config().postgres

    logger.info(f"Loading package from: {package_path}")
    tmp_path = None
    if package_path.startswith("http://") or package_path.startswith("https://"):
        logger.info("Remote URL detected — downloading to a temporary file first ...")
        fd, tmp_path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        urllib.request.urlretrieve(package_path, tmp_path)
        logger.info(f"Downloaded to {tmp_path} ({os.path.getsize(tmp_path):,} bytes)")
        local_path = tmp_path
    else:
        local_path = package_path

    try:
        package = Package(local_path)
        available = {r.name for r in package.resources}
        logger.info(f"Package contains {len(available)} resource(s): {sorted(available)}")

        if resource_name is not None:
            if resource_name not in available:
                raise ValueError(f"Resource '{resource_name}' not found in package. Available: {sorted(available)}")
            resources_to_load = [resource_name]
            logger.info(f"Single-resource mode: will load '{resource_name}' only")
        else:
            resources_to_load = [name for name in LOAD_ORDER if name in available]
            skipped = available - set(LOAD_ORDER)
            if skipped:
                logger.warning(f"Resources in package but not in LOAD_ORDER (will be skipped): {sorted(skipped)}")
            missing = set(LOAD_ORDER) - available
            if missing:
                logger.warning(f"Resources in LOAD_ORDER but not in package (will be skipped): {sorted(missing)}")
            logger.info(f"Will load {len(resources_to_load)} resource(s) in dependency order: {resources_to_load}")

        engine = get_postgres_engine(config)
        results = {}

        for name in resources_to_load:
            table_name = TABLE_PREFIX + name
            results[table_name] = _load_resource(package, name, table_name, engine)

        logger.info(f"Done. Loaded {len(results)} table(s): { {t: f'{n:,}' for t, n in results.items()} }")
        return results
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
            logger.debug(f"Removed temporary file {tmp_path}")


def _load_resource(package: Package, resource_name: str, table_name: str, engine) -> int:
    """Truncate `table_name` and insert all rows from `resource_name`.

    Returns the number of rows inserted.
    """
    logger.info(f"[{resource_name}] Reading rows from package ...")
    resource = package.get_resource(resource_name)
    rows = resource.read_rows()
    logger.info(f"[{resource_name}] Read {len(rows):,} rows")

    if not rows:
        logger.warning(f"[{resource_name}] No rows to insert — truncating '{table_name}' and leaving it empty")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        return 0

    field_names = list(rows[0].keys())
    col_names = ", ".join(field_names)
    placeholders = ", ".join(f":{k}" for k in field_names)

    rows_as_dicts = [dict(row) for row in rows]

    logger.info(f"[{resource_name}] Truncating '{table_name}' ...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        logger.info(f"[{resource_name}] Inserting {len(rows_as_dicts):,} rows into '{table_name}' ...")
        conn.execute(
            text(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"),
            rows_as_dicts,
        )

    logger.info(f"[{resource_name}] ✓ {len(rows_as_dicts):,} rows inserted into '{table_name}'")
    return len(rows_as_dicts)
