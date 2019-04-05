"""
Tests for extract_metadata.py

Uses pytest to setup fixtures for each group of tests.

References:
    - http://pythontesting.net/framework/pytest/pytest-fixtures-easy-example/
    - http://pythontesting.net/framework/pytest/pytest-xunit-style-fixtures/

"""

import collections
import datetime
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

@pytest.fixture(scope='module')
def setup_module(request):
    """
    Setup module-level fixtures.
    """

    # Create temporary database for testing.
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
    mock_params = MagicMock()
    mock_params.metabase_connection_string = conn_str
    mock_params.data_connection_string = conn_str

    def teardown_module():
        """
        Delete the temporary database.
        """
        postgresql.stop()

    request.addfinalizer(teardown_module)

    return_db = collections.namedtuple(
        'db',
        ['postgresql', 'engine', 'mock_params']
    )

    return return_db(
        postgresql=postgresql,
        engine=engine,
        mock_params=mock_params
    )


# #############################################################################
#   Test functions
# #############################################################################

#   Tests for `process_table()`
# =========================================================================

@pytest.fixture
def setup_empty_table(setup_module, request):
    """
    Setup function-level fixtures for 'process_table()'.
    """
    engine = setup_module.engine

    engine.execute("""
        INSERT INTO metabase.data_table (data_table_id, file_table_name) VALUES
            (1, 'data.col_level_meta');

        CREATE TABLE data.col_level_meta
            (c_num INT, c_text TEXT, c_code TEXT, c_date DATE);
    """)

    def teardown_empty_table():
        engine.execute("""
            TRUNCATE TABLE metabase.data_table CASCADE;
            DROP TABLE data.col_level_meta;
        """)

    request.addfinalizer(teardown_empty_table)


def test_empty_table(setup_module, setup_empty_table):
    """Test extracting column level metadata from an empy table."""

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)

    with pytest.raises(ValueError):
        extract.process_table(categorical_threshold=2)


@pytest.fixture
def setup_get_column_level_metadata(setup_module, request):
    """
    Setup function-level fixtures for `_get_column_level_metadata()`.
    """

    engine = setup_module.engine

    engine.execute("""
        INSERT INTO metabase.data_table (data_table_id, file_table_name) VALUES
            (1, 'data.col_level_meta');

        CREATE TABLE data.col_level_meta
            (c_num INT, c_text TEXT, c_code TEXT, c_date DATE);

        INSERT INTO data.col_level_meta (c_num, c_text, c_code, c_date) VALUES
            (1, 'abc',   'M', '2018-01-01'),
            (2, 'efgh',  'F', '2018-02-01'),
            (3, 'ijklm', 'F', '2018-03-02'),
            (NULL, NULL, NULL, NULL);
    """)

    def teardown_get_column_level_metadata():
        engine.execute("""
            TRUNCATE TABLE metabase.data_table CASCADE;
            DROP TABLE data.col_level_meta;
        """)

    request.addfinalizer(teardown_get_column_level_metadata)


def test_get_column_level_metadata_column_info(
        setup_module,
        setup_get_column_level_metadata):
    """Test extracting column level metadata into Column Info table."""

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)

    extract.process_table(categorical_threshold=2)

    # Check if the length of column info results equals to 4 columns.
    engine = setup_module.engine
    results = engine.execute('SELECT * FROM metabase.column_info').fetchall()

    assert 4 == len(results)


def test_get_column_level_metadata_numeric(
        setup_module,
        setup_get_column_level_metadata):
    """Test extracting numeric column level metadata."""

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)
    extract.process_table(categorical_threshold=2)

    engine = setup_module.engine
    results = engine.execute("""
        SELECT
            data_table_id,
            column_name,
            minimum,
            maximum,
            mean,
            median,
            updated_by,
            date_last_updated
        FROM metabase.numeric_column
    """).fetchall()[0]

    assert 1 == results['data_table_id']
    assert 'c_num' == results['column_name']
    assert 1 == results['minimum']
    assert 3 == results['maximum']
    assert 2 == results['mean']
    assert 2 == results['median']
    assert isinstance(results['updated_by'], str)
    assert isinstance(results['date_last_updated'], datetime.datetime)


def test_get_column_level_metadata_text(
        setup_module,
        setup_get_column_level_metadata):
    """Test extracting text column level metadata."""

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)
    extract.process_table(categorical_threshold=2)

    engine = setup_module.engine
    results = engine.execute("""
        SELECT
            data_table_id,
            column_name,
            max_length,
            min_length,
            median_length,
            updated_by,
            date_last_updated
        FROM metabase.text_column
    """).fetchall()[0]

    assert 1 == results['data_table_id']
    assert 'c_text' == results['column_name']
    assert 5 == results['max_length']
    assert 3 == results['min_length']
    assert 4 == results['median_length']
    assert isinstance(results['updated_by'], str)
    assert isinstance(results['date_last_updated'], datetime.datetime)


def test_get_column_level_metadata_date(
        setup_module,
        setup_get_column_level_metadata):
    """Test extracting date column level metadata."""

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)
    extract.process_table(categorical_threshold=2)

    engine = setup_module.engine
    results = engine.execute("""
        SELECT
            data_table_id,
            column_name,
            min_date,
            max_date,
            updated_by,
            date_last_updated
        FROM metabase.date_column
    """).fetchall()[0]

    assert 1 == results['data_table_id']
    assert 'c_date' == results['column_name']
    assert datetime.date(2018, 1, 1) == results['min_date']
    assert datetime.date(2018, 3, 2) == results['max_date']
    assert isinstance(results[4], str)
    assert isinstance(results[5], datetime.datetime)


def test_get_column_level_metadata_code(
        setup_module, setup_get_column_level_metadata):
    """Test extracting code column level metadata."""

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)
    extract.process_table(categorical_threshold=2)

    engine = setup_module.engine
    results = engine.execute("""
        SELECT
            data_table_id,
            column_name,
            code,
            frequency,
            updated_by,
            date_last_updated
        FROM metabase.code_frequency
    """).fetchall()

    assert 3 == len(results)

    assert 1 == results[0]['data_table_id']
    assert 'c_code' == results[0]['column_name']
    assert (results[0]['code'] in ('M', 'F', None))
    assert isinstance(results[0]['updated_by'], str)
    assert isinstance(results[0]['date_last_updated'], datetime.datetime)

    assert 1 == results[1]['data_table_id']
    assert 'c_code' == results[1]['column_name']
    assert (results[1]['code'] in ('M', 'F', None))
    assert isinstance(results[1]['updated_by'], str)
    assert isinstance(results[1]['date_last_updated'], datetime.datetime)

    assert 1 == results[2]['data_table_id']
    assert 'c_code' == results[2]['column_name']
    assert (results[2]['code'] in ('M', 'F', None))
    assert isinstance(results[2]['updated_by'], str)
    assert isinstance(results[2]['date_last_updated'], datetime.datetime)

    frequency_1 = results[0]['code'], results[0]['frequency']
    frequency_2 = results[1]['code'], results[1]['frequency']
    frequency_3 = results[2]['code'], results[2]['frequency']
    all_frequencies = set([frequency_1, frequency_2, frequency_3])
    expected = set([('M', 1), ('F', 2), (None, 1)])

    assert expected == all_frequencies
