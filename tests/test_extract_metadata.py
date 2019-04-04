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

#   Tests for `__get_table_name()`
# =========================================================================

@pytest.fixture()
def setup_get_table_name(setup_module, request):
    """
    Setup function-level fixtures for `__get_table_name()`.
    """

    engine = setup_module.engine

    engine.execute("""
        INSERT INTO metabase.data_table (data_table_id, file_table_name) VALUES
            (1, 'table_name_not_splitable'),
            (2, 'table.name.contain.extra.dot'),
            (3, 'data.data_table_name');
    """)

    def teardown_get_table_name():
        engine.execute('TRUNCATE TABLE metabase.data_table CASCADE')

    request.addfinalizer(teardown_get_table_name)


def test_get_table_name_data_table_id_not_found(
        setup_module,
        setup_get_table_name):
    with pytest.raises(ValueError):
        with patch(
                'metabase.extract_metadata.settings',
                setup_module.mock_params):
            extract_metadata.ExtractMetadata(data_table_id=0)


def test_get_table_name_file_table_name_not_splitable(setup_module,
                                                      setup_get_table_name):
    with pytest.raises(ValueError):
        with patch(
                'metabase.extract_metadata.settings',
                setup_module.mock_params):
            extract_metadata.ExtractMetadata(data_table_id=1)


def test_get_table_name_file_table_name_contain_extra_dot(
        setup_module,
        setup_get_table_name):
    with pytest.raises(ValueError):
        with patch(
                'metabase.extract_metadata.settings',
                setup_module.mock_params):
            extract_metadata.ExtractMetadata(data_table_id=2)


def test_get_table_name_one_data_table(
        setup_module,
        setup_get_table_name):
    with patch('metabase.extract_metadata.settings', setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=3)

    assert (('data', 'data_table_name')
            == (extract.schema_name, extract.table_name))


#   Tests for `_get_table_level_metadata()`
# =========================================================================

@pytest.fixture
def setup_get_table_level_metadata(setup_module, request):
    """
    Setup function-level fixtures for `_get_table_level_metadata()`.
    """

    engine = setup_module.engine
    engine.execute("""
        INSERT INTO metabase.data_table (data_table_id, file_table_name) VALUES
            (0, 'data.table_0_row'),
            (1, 'data.table_1_row_1_col'),
                -- Also used to test "update by" and "date last updated"

            (2, 'data.table_2_row_2_col');

        CREATE TABLE data.table_0_row (c1 INT PRIMARY KEY);

        CREATE TABLE data.table_1_row_1_col (c1 INT PRIMARY KEY);
        INSERT INTO data.table_1_row_1_col (c1) VALUES (1);

        CREATE TABLE data.table_2_row_2_col (c1 INT PRIMARY KEY, c2 TEXT);
        INSERT INTO data.table_2_row_2_col (c1, c2) VALUES (1, 'a'), (2, 'b');
    """)

    def teardown_get_table_level_metadata():
        engine.execute("""
            TRUNCATE TABLE metabase.data_table CASCADE;
            DROP TABLE data.table_0_row;
            DROP TABLE data.table_1_row_1_col;
            DROP TABLE data.table_2_row_2_col;
        """)

    request.addfinalizer(teardown_get_table_level_metadata)


def test_get_table_level_metadata_num_of_rows_0_row_raise_error(
        setup_module,
        setup_get_table_level_metadata):

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=0)

    with pytest.raises(ValueError):
        extract._get_table_level_metadata()


def test_get_table_level_metadata_1_row_1_col(
        setup_module,
        setup_get_table_level_metadata):

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=1)

    extract._get_table_level_metadata()

    engine = setup_module.engine
    result = engine.execute("""
        SELECT number_rows, number_columns
        FROM metabase.data_table
        WHERE data_table_id = 1
    """).fetchall()

    assert (1, 1) == result[0]


def test_get_table_level_metadata_2_row_2_col(
        setup_module,
        setup_get_table_level_metadata):

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(data_table_id=2)

    extract._get_table_level_metadata()

    engine = setup_module.engine
    result = engine.execute("""
        SELECT number_rows, number_columns
        FROM metabase.data_table
        WHERE data_table_id = 2
    """).fetchall()

    assert (2, 2) == result[0]


def test_get_table_level_metadata_updated_by_user_name_not_empty(
        setup_module,
        setup_get_table_level_metadata):
    test_data_table_id = 1

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(
            data_table_id=test_data_table_id)
    extract._get_table_level_metadata()

    engine = setup_module.engine
    result = engine.execute("""
        SELECT updated_by FROM metabase.data_table WHERE data_table_id = {}
    """.format(test_data_table_id)).fetchall()

    result_updated_by_user_name = result[0][0]

    assert (isinstance(result_updated_by_user_name, str)
            and (len(result_updated_by_user_name) > 0))


def test_get_table_level_metadata_date_last_updated_not_empty(
        setup_module,
        setup_get_table_level_metadata):

    test_data_table_id = 1

    with patch(
            'metabase.extract_metadata.settings',
            setup_module.mock_params):
        extract = extract_metadata.ExtractMetadata(
            data_table_id=test_data_table_id)
    extract._get_table_level_metadata()

    engine = setup_module.engine
    result = engine.execute("""
        SELECT date_last_updated
        FROM metabase.data_table
        WHERE data_table_id = {}
    """.format(test_data_table_id)).fetchall()

    assert isinstance(result[0]['date_last_updated'], datetime.datetime)


#   Tests for `_get_column_level_metadata()`
# =========================================================================

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
            (3, 'ijklm', 'F', '2018-03-02');
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

    extract._get_column_level_metadata(categorical_threshold=2)

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
    extract._get_column_level_metadata(categorical_threshold=2)

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
    extract._get_column_level_metadata(categorical_threshold=2)

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
    extract._get_column_level_metadata(categorical_threshold=2)

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
    extract._get_column_level_metadata(categorical_threshold=2)

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

    assert 1 == results[0]['data_table_id']
    assert 'c_code' == results[0]['column_name']
    assert (results[0]['code'] in ('M', 'F'))
    assert isinstance(results[0]['updated_by'], str)
    assert isinstance(results[0]['date_last_updated'], datetime.datetime)

    assert 1 == results[1]['data_table_id']
    assert 'c_code' == results[1]['column_name']
    assert (results[1]['code'] in ('M', 'F'))
    assert isinstance(results[1]['updated_by'], str)
    assert isinstance(results[1]['date_last_updated'], datetime.datetime)

    # Frequencies
    if results[0]['code'] == 'F':
        assert results[0]['frequency'] == 2
        assert results[1]['frequency'] == 1
    else:
        assert results[0]['frequency'] == 1
        assert results[1]['frequency'] == 2
