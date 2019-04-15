"""Class to extract metadata from a Data Table"""

import getpass

import psycopg2
from psycopg2 import sql

from . import settings
from . import extract_metadata_helper


class ExtractMetadata():
    """Class to extract metadata from a Data Table."""

    def __init__(self, data_table_id):
        """Set Data Table ID and connect to database.

        Args:
           data_table_id (int): ID associated with this Data Table.

        """
        self.data_table_id = data_table_id

        self.metabase_connection_string = settings.metabase_connection_string

        self.data_conn = psycopg2.connect(settings.data_connection_string)
        self.data_conn.autocommit = True
        self.data_cur = self.data_conn.cursor()

    def process_table(self, categorical_threshold=10,
                      date_format='YYYY-MM-DD'):
        """Update the metabase with metadata from this Data Table."""

        with psycopg2.connect(self.metabase_connection_string) as conn:
            with conn.cursor() as cursor:
                schema_name, table_name = self.__get_table_name(cursor)
                self._get_table_level_metadata(cursor, schema_name, table_name)
                self._get_column_level_metadata(
                    cursor,
                    schema_name,
                    table_name,
                    categorical_threshold,
                    date_format,
                )

        self.data_cur.close()
        self.data_conn.close()

    def _get_table_level_metadata(self, metabase_cur, schema_name, table_name):
        """Extract table level metadata and store it in the metabase.

        Extract table level metadata (number of rows, number of columns and
        file size (table size)) and store it in DataTable. Also set updated by
        and date last updated.

        Size is in bytes

        """
        self.data_cur.execute(
            sql.SQL('SELECT COUNT(*) as n_rows FROM {}.{};').format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        n_rows = self.data_cur.fetchone()[0]

        self.data_cur.execute(
            sql.SQL("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE
                    TABLE_SCHEMA = %s
                    AND TABLE_NAME = %s
            """),
            [schema_name, table_name]
        )
        n_cols = self.data_cur.fetchone()[0]

        self.data_cur.execute(
            sql.SQL('SELECT PG_RELATION_SIZE(%s);'),
            [schema_name + '.' + table_name],
        )
        table_size = self.data_cur.fetchone()[0]

        if n_rows == 0:
            raise ValueError('Selected data table has 0 rows.')
            # This will also capture n_cols == 0 and size == 0.

        metabase_cur.execute(
            """
                UPDATE metabase.data_table
                SET
                    number_rows = %(n_rows)s,
                    number_columns = %(n_cols)s,
                    size = %(table_size)s,
                    updated_by = %(user_name)s,
                    date_last_updated = (SELECT CURRENT_TIMESTAMP)
                WHERE data_table_id = %(data_table_id)s
                ;
            """,
            {
                'n_rows': n_rows,
                'n_cols': n_cols,
                'table_size': table_size,
                'user_name': getpass.getuser(),
                'data_table_id': self.data_table_id,
            }
        )

        # TODO: Update create_by and date_created
        # https://github.com/chapinhall/adrf-metabase/pull/8#discussion_r265339190

    def _get_column_level_metadata(self, metabase_cur, schema_name, table_name,
                                   categorical_threshold, date_format):
        """Extract column level metadata and store it in the metabase.

        Process columns one by one, identify or infer type, update Column Info
        and corresponding column table.

        """

        column_names = self.__get_column_names(schema_name, table_name)

        for col_name in column_names:
            column_data = self.__get_column_type(
                schema_name,
                table_name,
                col_name,
                categorical_threshold,
                date_format,
            )

            if column_data.type == 'numeric':
                self.__update_numeric_metadata(
                    metabase_cur,
                    col_name, column_data.data)
            elif column_data.type == 'text':
                self.__update_text_metadata(
                    metabase_cur,
                    col_name,
                    column_data.data)
            elif column_data.type == 'date':
                self.__update_date_metadata(
                    metabase_cur,
                    col_name,
                    column_data.data)
            elif column_data.type == 'code':
                self.__update_code_metadata(
                    metabase_cur,
                    col_name,
                    column_data.data)
            else:
                raise ValueError('Unknown column type')

    def __get_column_names(self, schema_name, table_name):
        """Returns the names of the columns in the data table.

        Returns:
            (str): Column names.

        """
        self.data_cur.execute(
                """
                SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = %(schema)s
                AND table_name  = %(table)s;
                """,
                {
                    'schema': schema_name,
                    'table': table_name
                },
                )

        columns = self.data_cur.fetchall()
        return([c[0] for c in columns])

    def __get_table_name(self, metabase_cur):
        """Return the the table schema and name using the Data Table ID.

        Returns table name and schema name by looking up the Data Table ID in
        the metabase. The table name and schema name will be used to query the
        table itself.

        Returns:
            (str, str): (schema name, table name)

        """
        metabase_cur.execute(
            """
            SELECT file_table_name
            FROM metabase.data_table
            WHERE data_table_id = %(data_table_id)s;
            """,
            {'data_table_id': self.data_table_id},
        )

        result = metabase_cur.fetchone()

        if result is None:
            raise ValueError('data_table_id not found in metabase.data_table')

        schema_name_table_name_tp = result[0].split('.')
        if len(schema_name_table_name_tp) != 2:
            raise ValueError('file_table_name is not in <schema>.<table> '
                             'format')

        return schema_name_table_name_tp

    def __get_column_type(self, schema_name, table_name, col,
                          categorical_threshold, date_format):
        """Identify or infer column type.

        Infers the column type.

        Returns:
          str: 'numeric', 'text', 'date' or 'code'

        """

        column_data = extract_metadata_helper.get_column_type(
            self.data_cur,
            col,
            categorical_threshold,
            schema_name,
            table_name,
            date_format,
        )

        return column_data

    def __update_numeric_metadata(self, metabase_cur, col_name, col_data):
        """Extract metadata from a numeric column.

        Extract metadata from a numeric column and store metadata in Column
        Info and Numeric Column. Update relevant audit fields.

        """

        extract_metadata_helper.update_numeric(
            metabase_cur,
            col_name,
            col_data,
            self.data_table_id,
        )

    def __update_text_metadata(self, metabase_cur, col_name, col_data):
        """Extract metadata from a text column.

        Extract metadata from a text column and store metadata in Column Info
        and Text Column. Update relevant audit fields.

        """

        extract_metadata_helper.update_text(
            metabase_cur,
            col_name,
            col_data,
            self.data_table_id,
        )

    def __update_date_metadata(self, metabase_cur, col_name, col_data):
        """Extract metadata from a date column.

        Extract metadata from date column and store metadate in Column Info and
        Date Column. Update relevant audit fields.

        """

        extract_metadata_helper.update_date(
            metabase_cur,
            col_name,
            col_data,
            self.data_table_id,
        )

    def __update_code_metadata(self, metabase_cur, col_name, col_data):
        """Extract metadata from a categorial column.

        Extract metadata from a categorial columns and store metadata in Column
        Info and Code Frequency. Update relevant audit fields.

        """
        # TODO: modify categorical_threshold to take percentage arguments.

        extract_metadata_helper.update_code(
            metabase_cur,
            col_name,
            col_data,
            self.data_table_id,
        )
