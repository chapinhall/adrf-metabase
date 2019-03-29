"""Tests for extract_metadata.py"""

import datetime
import unittest
from unittest.mock import MagicMock, patch

import alembic.config
from alembic.config import Config
import pytest
import sqlalchemy
import testing.postgresql

from metabase import extract_metadata

# #############################################################################
#   Module-level fixtures
# #############################################################################

def setup_module():
    """
    Setup module-level fixtures.
    """
    # Create temporary database for testing.
    global postgresql
    postgresql = testing.postgresql.Postgresql()
    connection_params = postgresql.dsn()

    # Create connection string from params.
    conn_str = 'postgresql://{user}@{host}:{port}/{database}'.format(
        user=connection_params['user'],
        host=connection_params['host'],
        port=connection_params['port'],
        database=connection_params['database'],
    )

    # Create `metabase` and `data` schemata.
    global engine
    engine = sqlalchemy.create_engine(conn_str)
    engine.execute(sqlalchemy.schema.CreateSchema('metabase'))
    engine.execute(sqlalchemy.schema.CreateSchema('data'))

    # Create metabase tables with alembic scripts.
    alembic_cfg = Config()
    alembic_cfg.set_main_option('script_location', 'alembic')
    alembic_cfg.set_main_option('sqlalchemy.url', conn_str)
    alembic.command.upgrade(alembic_cfg, 'head')

    # Mock settings to connect to testing database. Use this database for
    # both the metabase and data schemata.
    global mock_params
    mock_params = MagicMock()
    mock_params.metabase_connection_string = conn_str
    mock_params.data_connection_string = conn_str


def teardown_module():
    """
    Delete the temporary database.
    """
    postgresql.stop()


# #############################################################################
#   Test functions
# #############################################################################

#   Tests for `__get_table_name()`
# =========================================================================

@pytest.fixture
def setup_get_table_name(request):
    """
    Setup function-level fixtures for `__get_table_name()`.
    """
    engine.execute("""
        INSERT INTO metabase.data_table (data_table_id, file_table_name) VALUES
            (1, 'table_name_not_splitable'),
            (2, 'table.name.contain.extra.dot'),
            (3, 'data.data_table_name');
    """)

    def teardown_get_table_name():
        engine.execute('TRUNCATE TABLE metabase.data_table CASCADE')

    request.addfinalizer(teardown_get_table_name)


def test_get_table_name_data_table_id_not_found(setup_get_table_name):
    with pytest.raises(ValueError):
        with patch('metabase.extract_metadata.settings', mock_params):
            extract_metadata.ExtractMetadata(data_table_id=0)


def test_get_table_name_file_table_name_not_splitable(setup_get_table_name):
    with pytest.raises(ValueError):
        with patch('metabase.extract_metadata.settings', mock_params):
            extract_metadata.ExtractMetadata(data_table_id=1)


def test_get_table_name_file_table_name_contain_extra_dot(
        setup_get_table_name):
    with pytest.raises(ValueError):
        with patch('metabase.extract_metadata.settings', mock_params):
            extract_metadata.ExtractMetadata(data_table_id=2)


def test_get_table_name_one_data_table(setup_get_table_name):
    with patch('metabase.extract_metadata.settings', mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=3)

    assert (('data', 'data_table_name')
            == (extract.schema_name, extract.table_name))


#   Tests for `__get_table_name()`
# =========================================================================

@pytest.fixture
def setup_get_table_level_metadata(request):
    """
    Setup function-level fixtures for `_get_table_level_metadata()`.
    """
    engine.execute("""
        INSERT INTO metabase.data_table (data_table_id, file_table_name) VALUES
            (0, 'data.table_0_row'),
            (1, 'data.table_1_row_1_col')
        ;
        
        CREATE TABLE data.table_0_row (c1 INT PRIMARY KEY);

        CREATE TABLE data.table_1_row_1_col (c1 INT PRIMARY KEY);
        INSERT INTO data.table_1_row_1_col (c1) VALUES (1);
    """)

    def teardown_get_table_level_metadata():
        engine.execute("""
            TRUNCATE TABLE metabase.data_table CASCADE;
            DROP TABLE data.table_0_row;
            DROP TABLE data.table_1_row_1_col;
        """)

    request.addfinalizer(teardown_get_table_level_metadata)


def test_get_table_level_metadata_num_of_rows_0_row_raise_error(
        setup_get_table_level_metadata):
    with patch('metabase.extract_metadata.settings', mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=0)

    with pytest.raises(ValueError):
        extract._get_table_level_metadata()




