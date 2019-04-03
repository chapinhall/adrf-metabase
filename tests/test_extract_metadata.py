"""Tests for extract_metadata.py"""

import datetime
import unittest
from unittest.mock import MagicMock, patch

import alembic.config
from alembic.config import Config
import pytest
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
import testing.postgresql

from metabase import extract_metadata


class ExtractMetadataTest(unittest.TestCase):
    """Test for extract_metadata"""

    @classmethod
    def setUpClass(cls):
        """Create database fixtures."""

        # Create temporary database for testing.
        cls.postgresql = testing.postgresql.Postgresql()
        connection_params = cls.postgresql.dsn()

        # Create connection string from params.
        conn_str = 'postgresql://{user}@{host}:{port}/{database}'.format(
            user=connection_params['user'],
            host=connection_params['host'],
            port=connection_params['port'],
            database=connection_params['database'],
            )
        cls.connection_string = conn_str

        # Create `metabase` and `data` schemata.
        engine = sqlalchemy.create_engine(conn_str)
        engine.execute(sqlalchemy.schema.CreateSchema('metabase'))
        engine.execute(sqlalchemy.schema.CreateSchema('data'))
        cls.engine = engine

        # Create metabase tables with alembic scripts.
        alembic_cfg = Config()
        alembic_cfg.set_main_option('script_location', 'alembic')
        alembic_cfg.set_main_option('sqlalchemy.url', conn_str)
        alembic.command.upgrade(alembic_cfg, 'head')

        # TODO: Move database setup codes into fixtures.
        engine.execute('create table data.numeric_1 '
                       '(c1 int primary key, c2 numeric)')
        engine.execute('insert into data.numeric_1 values (1, 1.1)')
        engine.execute('insert into data.numeric_1 values (2, 2.2)')

        # TODO: create text, date, and categorical testing tables.

        # Mock settings to connect to testing database. Use this database for
        # both the metabase and data schemata.
        mock_params = MagicMock()
        mock_params.metabase_connection_string = conn_str
        mock_params.data_connection_string = conn_str
        cls.mock_params = mock_params

        # Generate mapped classes from database.
        Base = automap_base(
            bind=engine,
            metadata=sqlalchemy.MetaData(schema='metabase')
            )
        Base.prepare(engine, reflect=True)
        cls.data_table = Base.classes.data_table

    @classmethod
    def tearDownClass(cls):
        """Delete temporary database."""

        cls.postgresql.stop()

    def tearDown(self):
        self.engine.execute("TRUNCATE TABLE metabase.data_table CASCADE")
        self.engine.execute("TRUNCATE TABLE metabase.numeric_column CASCADE")
        self.engine.execute("TRUNCATE TABLE metabase.text_column")
        self.engine.execute("TRUNCATE TABLE metabase.date_column")
        self.engine.execute("TRUNCATE TABLE metabase.code_frequency")
        self.engine.execute('DROP TABLE IF EXISTS data.table_1')

    def test_get_table_name_data_table_id_not_found(self):
        """
        Test the validity of `data_table_id` as an argument to the constructor
        of ExtractMetadata.
        """
        with pytest.raises(ValueError):
            # Will raise error since `metabase.data_table` is empty
            with patch('metabase.extract_metadata.settings', self.mock_params):
                extract_metadata.ExtractMetadata(data_table_id=1)

    def test_get_table_name_file_table_name_not_splitable(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'unqualified_table_name');
        """)

        with pytest.raises(ValueError):
            with patch('metabase.extract_metadata.settings', self.mock_params):
                extract_metadata.ExtractMetadata(data_table_id=1)

    def test_get_table_name_file_table_name_contain_extra_dot(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'lots.of.dots');
        """)

        with pytest.raises(ValueError):
            with patch('metabase.extract_metadata.settings', self.mock_params):
                extract_metadata.ExtractMetadata(data_table_id=1)

    def test_get_table_name_one_data_table(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.data_table_name');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        assert (('data', 'data_table_name')
                == (extract.schema_name, extract.table_name))

    def test_get_table_name_multiple_data_table(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table
                (data_table_id, file_table_name)
                VALUES
                    (1, 'data.data_table_name_1'),
                    (2, 'data.data_table_name_2')
            ;
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=2)

        assert (('data', 'data_table_name_2')
                == (extract.schema_name, extract.table_name))

    def test_get_table_level_metadata_num_of_rows_0_row_raise_error(self):
        """
        The following group of tests share data table `data.table_test_n_rows`:

            - test_get_table_level_metadata_num_of_rows_0_row_raise_error
            - test_get_table_level_metadata_num_of_rows_1_row
            - test_get_table_level_metadata_num_of_rows_2_rows

        `data.table_test_n_rows` will be dropped at the end of the last test
        in this group.

        """
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_rows');

            CREATE TABLE data.table_test_n_rows (c1 INT PRIMARY KEY);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_num_of_rows_1_row(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_rows');

            INSERT INTO data.table_test_n_rows (c1)
                VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        result = self.engine.execute("""
            SELECT number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_rows = result[0][0]

        assert 1 == result_n_rows

    def test_get_table_level_metadata_num_of_rows_2_rows(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_rows');

            INSERT INTO data.table_test_n_rows (c1)
                VALUES (2);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_n_rows;')

        result = self.engine.execute("""
            SELECT number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_rows = result[0][0]

        assert 2 == result_n_rows

    def test_get_table_level_metadata_num_of_cols_0_col_raise_error(self):
        """
        The following group of tests share data table `data.table_test_n_cols`:

            - test_get_table_level_metadata_num_of_cols_0_col_raise_error
            - test_get_table_level_metadata_num_of_cols_1_col_0_row_raise_error
            - test_get_table_level_metadata_num_of_cols_1_col_1_row
            - test_get_table_level_metadata_num_of_cols_2_cols_2_row

        `data.table_test_n_cols` will be dropped at the end of the last test
        in this group.

        """
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            CREATE TABLE data.table_test_n_cols ();
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_num_of_cols_1_col_0_row_raise_error(
            self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            ALTER TABLE data.table_test_n_cols ADD c1 INT PRIMARY KEY;
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        with pytest.raises(ValueError):
            extract._get_table_level_metadata()

    def test_get_table_level_metadata_num_of_cols_1_col_1_row(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            INSERT INTO data.table_test_n_cols (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        result = self.engine.execute("""
            SELECT number_columns, number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_cols_n_rows = result[0]

        assert (1, 1) == result_n_cols_n_rows

    def test_get_table_level_metadata_num_of_cols_2_cols_2_row(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_n_cols');

            ALTER TABLE data.table_test_n_cols ADD c2 TEXT;

            INSERT INTO data.table_test_n_cols (c1, c2) VALUES (2, 'text');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_n_cols;')

        result = self.engine.execute("""
            SELECT number_columns, number_rows
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_n_cols_n_rows = result[0]

        assert (2, 2) == result_n_cols_n_rows

    def test_get_table_level_metadata_updated_by_user_name_not_empty(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_updated_by');

            CREATE TABLE data.table_test_updated_by (c1 INT);

            INSERT INTO data.table_test_updated_by (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_updated_by;')

        result = self.engine.execute("""
            SELECT updated_by
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_updated_by_user_name = result[0][0]

        assert (isinstance(result_updated_by_user_name, str)
                and (len(result_updated_by_user_name) > 0))

    def test_get_table_level_metadata_date_last_updated_not_empty(self):
        self.engine.execute("""
            INSERT INTO metabase.data_table (data_table_id, file_table_name)
                VALUES (1, 'data.table_test_date_last_updated');

            CREATE TABLE data.table_test_date_last_updated (c1 INT);

            INSERT INTO data.table_test_date_last_updated (c1) VALUES (1);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_table_level_metadata()

        self.engine.execute('DROP TABLE data.table_test_date_last_updated;')

        result = self.engine.execute("""
            SELECT date_last_updated
            FROM metabase.data_table
            WHERE data_table_id = 1
        """).fetchall()

        result_date_last_updated = result[0][0]

        assert isinstance(result_date_last_updated, datetime.datetime)

    def test_get_column_level_metadata__column_info(self):
        """Test extracting column level metadata into Column Info table."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1
               (c_num INT, c_text TEXT, c_code TEXT, c_date DATE);

           INSERT INTO data.table_1 (c_num, c_text, c_code, c_date)
           VALUES
           (1, 'text_1', 'code_1', '2018-01-01'),
           (2, 'text_2', 'code_1', '2018-02-01'),
           (3, 'text_3', 'code_2', '2018-03-02');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check column info retuls.
        results = self.engine.execute(
            "SELECT * FROM metabase.column_info"
        ).fetchall()

        assert 4 == len(results)

    def test_get_column_level_metadata_numeric(self):
        """Test extracting numeric column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1
               (c_num INT);

           INSERT INTO data.table_1 (c_num)
           VALUES
           (1),
           (2),
           (3);
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check Numeric results.
        results = self.engine.execute("""
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

        assert results[0] == 1
        assert results[1] == 'c_num'
        assert results[2] == 1
        assert results[3] == 3
        assert results[4] == 2
        assert results[5] == 2
        assert isinstance(results[6], str)
        assert isinstance(results[7], datetime.datetime)

    def test_get_column_level_metadata_text(self):
        """Test extracting text column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1 (c_text TEXT);

           INSERT INTO data.table_1 (c_text)
           VALUES
           ('abc'),
           ('efgh'),
           ('ijklm');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check date results.
        results = self.engine.execute("""
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

        assert results[0] == 1
        assert results[1] == 'c_text'
        assert results[2] == 5
        assert results[3] == 3
        assert results[4] == 4
        assert isinstance(results[5], str)
        assert isinstance(results[6], datetime.datetime)

    def test_get_column_level_metadata_date(self):
        """Test extracting date column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1
               (c_date DATE);

           INSERT INTO data.table_1 ( c_date)
           VALUES
           ('2018-01-01'),
           ('2018-02-01'),
           ('2018-03-02');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        # Check date results.
        results = self.engine.execute("""
            SELECT
            data_table_id,
            column_name,
            min_date,
            max_date,
            updated_by,
            date_last_updated
            FROM metabase.date_column
        """).fetchall()[0]

        assert results[0] == 1
        assert results[1] == 'c_date'
        assert results[2] == datetime.date(2018, 1, 1)
        assert results[3] == datetime.date(2018, 3, 2)
        assert isinstance(results[4], str)
        assert isinstance(results[5], datetime.datetime)

    def test_get_column_level_metadata_code(self):
        """Test extracting code column level metadata."""

        self.engine.execute("""
           INSERT INTO metabase.data_table (data_table_id, file_table_name)
           VALUES (1, 'data.table_1');

           CREATE TABLE data.table_1 (c_code TEXT);

           INSERT INTO data.table_1 (c_code)
            VALUES
                ('M'),
                ('F'),
                ('F');
        """)

        with patch('metabase.extract_metadata.settings', self.mock_params):
            extract = extract_metadata.ExtractMetadata(data_table_id=1)

        extract._get_column_level_metadata(categorical_threshold=2)

        results = self.engine.execute("""
            SELECT
                data_table_id,
                column_name,
                code,
                frequency,
                updated_by,
                date_last_updated
            FROM metabase.code_frequency
        """).fetchall()

        for i in results:
            print(i)

        assert results[0][0] == 1
        assert results[0][1] == 'c_code'
        assert(results[0][2] in ('M','F'))
        assert isinstance(results[0][4], str)
        assert isinstance(results[0][5], datetime.datetime)

        assert results[1][0] == 1
        assert results[1][1] == 'c_code'
        assert(results[1][2] in ('M', 'F'))
        assert isinstance(results[1][4], str)
        assert isinstance(results[1][5], datetime.datetime)

        # Frequencies
        if results[0][2] == 'F':
            assert results[0][3] == 2
            assert results[1][3] == 1
        else:
            assert results[0][3] == 1
            assert results[1][3] == 2
