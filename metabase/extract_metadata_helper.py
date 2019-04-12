"""Helper funtions for extract_metadata.
"""

from collections import namedtuple, Counter
import getpass
import json
import statistics

import psycopg2
from psycopg2 import sql


def get_column_type(data_cursor, col, categorical_threshold, schema_name,
                    table_name):
    """Return the column type and the contents of the column."""

    col_type = ''
    data = []

    numeric_flag, numeric_data = is_numeric(data_cursor, col, schema_name,
                                            table_name)
    date_flag, date_data = is_date(data_cursor, col, schema_name, table_name)
    code_flag, code_data = is_code(data_cursor, col, schema_name, table_name,
                                   categorical_threshold)

    if numeric_flag:
        col_type = 'numeric'
        data = numeric_data
    elif date_flag:
        col_type = 'date'
        data = date_data
    elif code_flag:
        col_type = 'code'
        data = code_data
    else:
        col_type = 'text'
        data = code_data  # If is_code is False, column assumed to be text.

    column_data = namedtuple('column_data', ['type', 'data'])
    return column_data(col_type, data)


def is_numeric(data_cursor, col, schema_name, table_name):
    """Return True and contents of column if column is numeric.
    """

    try:
        data_cursor.execute(
            sql.SQL("""
            SELECT {}::NUMERIC FROM {}.{}
            """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        data = [i[0] for i in data_cursor.fetchall()]
        flag = True
    except (psycopg2.ProgrammingError, psycopg2.DataError):
        data_cursor.execute('DROP TABLE IF EXISTS converted_data')
        data = []
        flag = False

    return flag, data


def is_date(data_cursor, col, schema_name, table_name):
    """Return True and contents of column if column is date.
    """

    try:
        data_cursor.execute(
            sql.SQL("""
            SELECT {}::DATE FROM {}.{}
            """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        data = [i[0] for i in data_cursor.fetchall()]
        flag = True
    except (psycopg2.ProgrammingError, psycopg2.DataError):
        data_cursor.execute("DROP TABLE IF EXISTS converted_data")
        data = []
        flag = False

    return flag, data


def is_code(data_cursor, col, schema_name, table_name,
            categorical_threshold):
    """Return True and contents of column if column is categorical.
    """

    data_cursor.execute(
        sql.SQL(
            """
            SELECT COUNT(DISTINCT {}) FROM {}.{}
            """).format(
            sql.Identifier(col),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
    )
    n_distinct = data_cursor.fetchall()[0][0]

    data_cursor.execute(sql.SQL("""
        SELECT {} FROM {}.{}
        """).format(
                sql.Identifier(col),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
        )
        )
    data = [i[0] for i in data_cursor.fetchall()]

    if n_distinct <= categorical_threshold:
        flag = True
    else:
        flag = False

    return flag, data


def update_numeric(metabase_cursor, col_name, col_data, data_table_id):
    """Update Column Info and Numeric Column for a numerical column."""

    serial_column_id = update_column_info(metabase_cursor, col_name,
                                          data_table_id, 'numeric')
    # TODO: Update created by, created date.

    numeric_stats = get_numeric_metadata(col_data)

    metabase_cursor.execute(
        """
        INSERT INTO metabase.numeric_column (
            column_id,
            data_table_id,
            column_name,
            minimum,
            maximum,
            mean,
            median,
            updated_by,
            date_last_updated
        ) VALUES (
            %(column_id)s,
            %(data_table_id)s,
            %(column_name)s,
            %(minimum)s,
            %(maximum)s,
            %(mean)s,
            %(median)s,
            %(updated_by)s,
            (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'column_id': serial_column_id,
            'data_table_id': data_table_id,
            'column_name': col_name,
            'minimum': numeric_stats.min,
            'maximum': numeric_stats.max,
            'mean': numeric_stats.mean,
            'median': numeric_stats.median,
            'updated_by': getpass.getuser(),
        }
    )


def get_numeric_metadata(col_data):
    """Get metdata from a numeric column."""

    not_null_num_ls = [num for num in col_data if num is not None]

    if not_null_num_ls:
        mean = statistics.mean(not_null_num_ls)
        median = statistics.median(not_null_num_ls)
        max_col = max(not_null_num_ls)
        min_col = min(not_null_num_ls)
    else:
        mean = None
        median = None
        max_col = None
        min_col = None

    numeric_stats = namedtuple(
        'numeric_stats',
        ['min', 'max', 'mean', 'median'],
    )
    return numeric_stats(min_col, max_col, mean, median)


def update_text(metabase_cursor, col_name, col_data, data_table_id):
    """Update Column Info  and Numeric Column for a text column."""

    serial_column_id = update_column_info(metabase_cursor, col_name,
                                          data_table_id, 'text')
    # Update created by, created date.

    (max_len, min_len, median_len) = get_text_metadata(col_data)

    metabase_cursor.execute(
        """
        INSERT INTO metabase.text_column
        (
        column_id,
        data_table_id,
        column_name,
        max_length,
        min_length,
        median_length,
        updated_by,
        date_last_updated
        )
        VALUES
        (
        %(column_id)s,
        %(data_table_id)s,
        %(column_name)s,
        %(max_length)s,
        %(min_length)s,
        %(median_length)s,
        %(updated_by)s,
        (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'column_id': serial_column_id,
            'data_table_id': data_table_id,
            'column_name': col_name,
            'max_length': max_len,
            'min_length': min_len,
            'median_length': median_len,
            'updated_by': getpass.getuser(),
        }
    )


def get_text_metadata(col_data):
    """Get metadata from a text column."""

    try:
        col_data.remove(None)
    except ValueError:
        pass

    not_null_text_ls = [text for text in col_data if text is not None]

    if not_null_text_ls:
        text_lens_ls = [len(text) for text in not_null_text_ls]
        min_len = min(text_lens_ls)
        max_len = max(text_lens_ls)
        median_len = statistics.median(text_lens_ls)
    else:
        # Will only be needed if categorical_threshold = 0
        min_len = None
        max_len = None
        median_len = None

    return (max_len, min_len, median_len)


def update_date(metabase_cursor, col_name, col_data,
                data_table_id):
    """Update Column Info and Date Column for a date column."""

    try:
        col_data.remove(None)
    except ValueError:
        pass

    serial_column_id = update_column_info(metabase_cursor, col_name,
                                          data_table_id, 'date')

    (minimum, maximum) = get_date_metadata(col_data)

    metabase_cursor.execute(
        """
        INSERT INTO metabase.date_column
        (
        column_id,
        data_table_id,
        column_name,
        min_date,
        max_date,
        updated_by,
        date_last_updated
        )
        VALUES
        (
        %(column_id)s,
        %(data_table_id)s,
        %(column_name)s,
        %(min_date)s,
        %(max_date)s,
        %(updated_by)s,
        (SELECT CURRENT_TIMESTAMP)
        )
        """,
        {
            'column_id': serial_column_id,
            'data_table_id': data_table_id,
            'column_name': col_name,
            'min_date': minimum,
            'max_date': maximum,
            'updated_by': getpass.getuser(),
        }
        )


def get_date_metadata(col_data):
    """Get metadata from a date column."""

    try:
        col_data.remove(None)
    except ValueError:
        pass

    min_date = min(col_data)
    max_date = max(col_data)

    return (min_date, max_date)


def update_code(metabase_cursor, col_name, col_data,
                data_table_id):
    """Update Column Info and Code Frequency for a categorical column."""

    serial_column_id = update_column_info(metabase_cursor, col_name,
                                          data_table_id, 'code')

    code_counter = get_code_metadata(col_data)

    for code, frequency in code_counter.items():
        metabase_cursor.execute(
            """
            INSERT INTO metabase.code_frequency (
                column_id,
                data_table_id,
                column_name,
                code,
                frequency,
                updated_by,
                date_last_updated
            ) VALUES (
                %(column_id)s,
                %(data_table_id)s,
                %(column_name)s,
                %(code)s,
                %(frequency)s,
                %(updated_by)s,
               (SELECT CURRENT_TIMESTAMP)
            )
            """,
            {
                'column_id': serial_column_id,
                'data_table_id': data_table_id,
                'column_name': col_name,
                'code': code,
                'frequency': frequency,
                'updated_by': getpass.getuser(),
            },
        )


def get_code_metadata(col_data):

    code_frequecy_counter = Counter(col_data)

    return code_frequecy_counter


def update_column_info(cursor, col_name, data_table_id, data_type):
    """Add a row for this data column to the column info metadata table."""

    # TODO How to handled existing rows?

    # Create Column Info entry
    cursor.execute(
        """
        INSERT INTO metabase.column_info (
            data_table_id,
            column_name,
            data_type,
            updated_by,
            date_last_updated
        )
        VALUES (
            %(data_table_id)s,
            %(column_name)s,
            %(data_type)s,
            %(updated_by)s,
            (SELECT CURRENT_TIMESTAMP)
        )
        RETURNING column_id
        ;
        """,
        {
            'data_table_id': data_table_id,
            'column_name': col_name,
            'data_type': data_type,
            'updated_by': getpass.getuser(),
        }
    )

    serial_column_id = cursor.fetchall()[0][0]
    return serial_column_id


# #############################################################################
#   Called by `ExtractMetadata.export_table_metadata()`
# #############################################################################

def select_table_level_gmeta_fields(metabase_cur, data_table_id):
    """
    """
    date_format_str = 'YYYY-MM-DD'

    metabase_cur.execute(
        """
            SELECT
                file_table_name AS file_name,
                data_table.data_set_id AS dataset_id,
                -- data_set.title AS title,
                -- data_set.description AS description,
                TO_CHAR(start_date, %(date_format_str)s)
                    AS temporal_coverage_start,
                TO_CHAR(end_date, %(date_format_str)s) AS temporal_coverage_end,
                -- geographical_coverage
                -- geographical_unit
                -- data_set.keywords AS keywords,
                -- data_set.category AS category,
                -- data_set.document_link AS reference_url,
                contact AS data_steward,
                -- data_set.data_set_contact AS data_steward_organization,
                size AS file_size
                -- number_rows AS rows    NOTE: not included in the sample file
                -- number_columns AS columns  NOTE: not included in the sample file
            FROM metabase.data_table
                -- JOIN metabase.data_set USING (data_set_id)
            WHERE data_table_id = %(data_table_id)s
        """, 
        {
            'date_format_str': date_format_str,
            'data_table_id': data_table_id
        },
    )

    table_level_gmeta_fields_dict = metabase_cur.fetchall()[0]
    # Index by 0 since the result is a list of one dict.

    return table_level_gmeta_fields_dict


def select_column_level_gmeta_fields(metabase_cur, data_table_id):
    """
    """
    metabase_cur.execute(
        """
            SELECT column_id, column_name, data_type
            FROM metabase.column_info
            WHERE data_table_id = %(data_table_id)s;
        """,
        {
            'data_table_id': data_table_id,
        },
    )

    column_id_name_type_tp_ls = metabase_cur.fetchall()

    for column_id, _column_name, data_type in column_id_name_type_tp_ls:
        if data_type == 'numeric':
            column_gmeta_fields_dict = select_numeric_gmeta_fields(
                metabase_cur,
                column_id,
            )




    if numeric_flag:
        col_type = 'numeric'
        data = numeric_data
    elif date_flag:
        col_type = 'date'
        data = date_data
    elif code_flag:
        col_type = 'code'
        data = code_data
    else:
        col_type = 'text'



    return_ls.append(table_level_gmeta_fields_dict)

    return return_ls


def select_numeric_gmeta_fields(metabase_cur, column_id):
    """
    """
    metabase_cur.execute(
        """
        SELECT
            minimum,
            maximum,
            mean
        FROM metabase.numeric_column
        WHERE column_id = %
        """)




def test(metabase_cur, data_table_id):
    metabase_cur.execute("""
        SELECT column_id, column_name
        FROM metabase.column_info
        WHERE data_table_id = {data_table_id}
    """.format(data_table_id=data_table_id))

    
    return metabase_cur.fetchall()


def shape_gmeta_in_json(gmeta_fields_dict, output_filepath):
    """
    Shape and export GMETA fields in JSON format.

    TODO: Reshape the structure of the output JSON to match the sample format.
    """
    output_dict = {
        'file_name': gmeta_fields_dict['file_name'],
        'dataset_id': None,
        'columns_metadata': {},
    }


    # for col_name,     
    # output_dict['columns_metadata'][]

    
    with open(output_filepath, 'w') as output_file:
        json.dump(output_dict, output_file, indent=4, sort_keys=True)
