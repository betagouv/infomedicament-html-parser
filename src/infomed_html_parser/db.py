"""Database operations for CIS mapping."""

import os

import pymysql
import pymysql.cursors

from .config import DatabaseConfig, get_config


def get_filename_to_cis_mapping(config: DatabaseConfig | None = None) -> dict[str, str]:
    """
    Retrieve the filename -> CIS mapping from the database.

    Args:
        config: Database configuration. If None, uses config from environment.

    Returns:
        Dict mapping filenames to CIS codes.
    """
    if config is None:
        config = get_config().database

    connexion = pymysql.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port,
        cursorclass=pymysql.cursors.DictCursor,
    )
    mapping = {}
    try:
        with connexion.cursor() as cursor:
            cursor.execute("""
                SELECT
                  sd.SpecId AS cis,
                  d.DocPath AS filename
                FROM
                  Spec_Doc sd
                JOIN
                  Document d ON sd.DocId = d.DocId
            """)
            for row in cursor.fetchall():
                basename = os.path.basename(row["filename"])
                mapping[basename] = row["cis"]
    finally:
        connexion.close()
    return mapping


def get_authorized_cis(config: DatabaseConfig | None = None) -> set[str]:
    """
    Retrieve the list of authorized CIS codes from the database.

    Returns the SpecId of all specialties where isBdm is true.

    Args:
        config: Database configuration. If None, uses config from environment.

    Returns:
        Set of authorized CIS codes.
    """
    if config is None:
        config = get_config().database

    connexion = pymysql.connect(
        host=config.host,
        user=config.user,
        password=config.password,
        database=config.database,
        port=config.port,
        cursorclass=pymysql.cursors.DictCursor,
    )
    cis_set = set()
    try:
        with connexion.cursor() as cursor:
            cursor.execute("SELECT SpecId FROM Specialite WHERE isBdm")
            for row in cursor.fetchall():
                cis_set.add(str(row["SpecId"]))
    finally:
        connexion.close()
    return cis_set
