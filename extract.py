"""Main script for extracting metadata from a table.

This script includes some set up to handle the data receipt part of the
metabase that has not be implemented yet including updating metabase.data_table
with a new data_table_id and the table name (including schema). The script then
extracts metadata from this table and updates metabase.column_info,
metabase.numeric_column, metabase.date_columns and metabase.code_frequency
as appropriate.

The new data_table_is displayed when the script is run. Following queries in
PostgreSQL will show the updates to the metabase:

select * from metabase.column_info where data_table_id = <data_table_id>;
select * from metabase.numeric_column where data_table_id = <data_table_id>;
select * from metabase.text_column where data_table_id =  <data_table_id>;
select * from metabase.date_column where data_table_id = <data_table_id>;
select * from metabase.code_frequency where data_table_id = <data_table_id>;

"""

import argparse

import sqlalchemy

from metabase import extract_metadata


parser = argparse.ArgumentParser()

parser.add_argument(
    '-s', '--schema', type=str, required=True,
    help='Schema name of the data')
parser.add_argument(
    '-t', '--table', type=str, required=True,
    help='Table name of the data')
parser.add_argument(
    '-c', '--categorical', type=int, default=10,
    help='Max number of distinct values in all categorical columns')

args = parser.parse_args()
schema_name = args.schema
table_name = args.table
categorical_threshold = args.categorical

full_table_name = schema_name + '.' + table_name

engine = sqlalchemy.create_engine('postgres://metaadmin@localhost/postgres')

# Update meatabase.data_table with this new table.
max_id = engine.execute(
    'SELECT MAX(data_table_id) FROM metabase.data_table'
    ).fetchall()[0][0]
if max_id is None:
    new_id = 1
else:
    new_id = max_id + 1
print("data_table_id is {} for table {}".format(new_id, full_table_name))

engine.execute(
    """
    INSERT INTO metabase.data_table
    (
    data_table_id,
    file_table_name
    )
    VALUES
    (
    %(data_table_id)s,
    %(file_table_name)s
    )
    """,
    {
        'data_table_id': new_id,
        'file_table_name': full_table_name
    }
)

# Extract metadata from data.
extract = extract_metadata.ExtractMetadata(data_table_id=new_id)
extract.process_table(categorical_threshold=categorical_threshold)
