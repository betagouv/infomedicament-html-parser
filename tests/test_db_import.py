"""Tests for DB import utilities, converted from infomedicament JS tests."""

import json
from types import SimpleNamespace

from infomedicament_dataeng.db import _insert_content_blocks, _upsert_semantic_document, get_clean_html


class TestGetCleanHTML:
    def test_no_anchor_tags(self):
        html = '<p class="AmmCorpsTexte">Simple text</p>'
        assert get_clean_html(html) == html

    def test_removes_a_name_keeps_content(self):
        html = '<p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>'
        expected = '<p class="AmmAnnexeTitre">NOTICE</p>'
        assert get_clean_html(html) == expected

    def test_multiple_a_name_tags(self):
        html = '<p><a name="first">Premier</a></p><p><a name="second">Deuxième</a></p>'
        expected = "<p>Premier</p><p>Deuxième</p>"
        assert get_clean_html(html) == expected

    def test_nested_html_in_a_name(self):
        html = '<p><a name="_Toc123"><span class="bold">Titre</span> avec <em>emphase</em></a></p>'
        expected = '<p><span class="bold">Titre</span> avec <em>emphase</em></p>'
        assert get_clean_html(html) == expected

    def test_empty_a_name_tag(self):
        html = '<p><a name=""></a>Some text</p>'
        expected = "<p>Some text</p>"
        assert get_clean_html(html) == expected

    def test_preserves_href_anchors(self):
        html = '<p><a href="https://example.com">Link</a></p>'
        assert get_clean_html(html) == html

    def test_real_world_notice_example(self):
        html = '<p class="AmmAnnexeTitre"><a name="Ann3bNotice">NOTICE</a></p>'
        expected = '<p class="AmmAnnexeTitre">NOTICE</p>'
        assert get_clean_html(html) == expected

    def test_a_name_at_start_of_string(self):
        html = '<a name="start">Content</a>'
        assert get_clean_html(html) == "Content"


class TestInsertContentBlocks:
    def test_inserts_blocks_and_returns_ids(self, fake_connection):
        conn = fake_connection(ids=[1, 2, 3])
        result = _insert_content_blocks(
            conn,
            "notices_content",
            [
                {"content": "Bloc 1"},
                {"content": "Bloc 2"},
                {"content": "Bloc ABCD"},
            ],
        )
        assert result == [1, 2, 3]
        assert len(conn.execute_calls) == 3

    def test_filters_blocks_without_content_children_or_text(self, fake_connection):
        conn = fake_connection(ids=[1])
        result = _insert_content_blocks(
            conn,
            "notices_content",
            [
                {"content": "Valid item"},
                {"type": "TypeOnly"},  # no content, children, text → filtered
                {"html": "<p>Test</p>"},  # no content, children, text → filtered
            ],
        )
        assert len(conn.execute_calls) == 1
        assert result == [1]

    def test_returns_empty_when_all_filtered(self, fake_connection):
        conn = fake_connection()
        result = _insert_content_blocks(
            conn,
            "notices_content",
            [
                {"type": "empty"},
                {"html": "only html"},
            ],
        )
        assert conn.execute_calls == []
        assert result == []

    def test_cleans_html_for_non_table_blocks(self, fake_connection):
        conn = fake_connection(ids=[1])
        _insert_content_blocks(
            conn,
            "notices_content",
            [
                {"content": "text", "html": '<p><a name="test">Content</a></p>'},
            ],
        )
        params = conn.execute_calls[-1]
        assert params["html"] == "<p>Content</p>"

    def test_does_not_clean_html_for_table_blocks(self, fake_connection):
        conn = fake_connection(ids=[1])
        dirty_html = '<p><a name="test">Content</a></p>'
        # table block uses children to pass the content filter
        _insert_content_blocks(
            conn,
            "notices_content",
            [
                {"type": "table", "html": dirty_html,
                    "children": [{"content": "cell"}]},
            ],
        )
        params = conn.execute_calls[-1]
        assert params["html"] == dirty_html  # not cleaned

    def test_table_block_does_not_recurse_children(self, fake_connection):
        conn = fake_connection(ids=[1])
        _insert_content_blocks(
            conn,
            "notices_content",
            [
                {"type": "table", "html": "<table/>",
                    "children": [{"content": "cell"}]},
            ],
        )
        # only the table itself is inserted, not the child cell
        assert len(conn.execute_calls) == 1


def test_upsert_semantic_notice_writes_html_date_and_indication(fake_connection):
    conn = fake_connection()

    _upsert_semantic_document(
        conn,
        "notices",
        {
            "cis": "61234567",
            "content_html": "<p>Notice</p>",
            "date_notif": "2026-07-17",
            "indication": "Traitement de la douleur.",
        },
    )

    assert conn.execute_calls == [
        {
            "cis": 61234567,
            "content_html": "<p>Notice</p>",
            "date_notif": "2026-07-17",
        },
        {
            "cis": 61234567,
            "description": "Traitement de la douleur.",
        },
    ]


def test_upsert_semantic_rcp_writes_html_and_date_without_updating_description(fake_connection):
    conn = fake_connection()

    _upsert_semantic_document(
        conn,
        "rcp",
        {
            "cis": "61234567",
            "content_html": "<p>RCP</p>",
            "date_notif": "2026-07-17",
            "indication": "Must not be written",
        },
    )

    assert conn.execute_calls == [
        {
            "cis": 61234567,
            "content_html": "<p>RCP</p>",
            "date_notif": "2026-07-17",
        }
    ]


def test_upsert_semantic_document_rejects_unknown_table(fake_connection):
    conn = fake_connection()

    try:
        _upsert_semantic_document(
            conn, "other", {"cis": "61234567", "content_html": "<p>Document</p>"})
    except ValueError as error:
        assert str(error) == "Unsupported semantic document table: other"
    else:
        raise AssertionError(
            "Unknown semantic document table should be rejected")


def test_db_import_auto_detects_semantic_centralise_records(monkeypatch):
    from infomedicament_dataeng import cli

    record = {"cis": "61234567", "content_html": "<p>Notice</p>",
              "date_notif": None, "indication": "Pain"}

    class FakeS3:
        def list_parsed_files(self, pattern, since=None):
            return ["exports/parsed_N_semantic.jsonl"]

        def download_file_content(self, key):
            return json.dumps(record).encode()

    semantic_calls = []
    legacy_calls = []
    monkeypatch.setattr(cli, "make_s3_client", FakeS3)
    monkeypatch.setattr(cli, "get_config",
                        lambda: SimpleNamespace(postgres=object()))
    monkeypatch.setattr(
        cli,
        "import_semantic_documents",
        lambda records, table, config: (
            semantic_calls.append((list(records), table)) or (1, 0)),
    )
    monkeypatch.setattr(
        cli,
        "import_to_postgres",
        lambda records, main_table, content_table, config: (
            legacy_calls.append(list(records)) or (0, 0)),
    )

    cli.db_import("N")

    assert semantic_calls == [([record], "notices")]
    assert legacy_calls == []
