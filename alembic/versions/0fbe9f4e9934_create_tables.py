"""create tables

Revision ID: 0fbe9f4e9934
Revises:
Create Date: 2019-02-05 13:13:49.631921

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0fbe9f4e9934'
down_revision = None
branch_labels = None
depends_on = None

SCHEMA_NAME = 'metabase'


def upgrade():
    '''Create tables and associated constraints and types.'''

    op.create_table(
        'data_request',
        sa.Column('request_id', sa.Integer, primary_key=True),
        sa.Column('data_source_id', sa.Integer),
        sa.Column('notes', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'contact_history',
        sa.Column('request_id', sa.Integer),
        sa.Column('internal_contact', sa.TEXT),
        sa.Column('date', sa.TIMESTAMP),
        sa.Column('notes', sa.Text),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_source',
        sa.Column('data_source_id', sa.Integer, primary_key=True),
        sa.Column('data_source_descriptions', sa.Text),
        sa.Column('agency_contact', sa.Text),
        sa.Column('data_source_state', sa.Text),
        sa.Column('data_source_county', sa.Text),
        sa.Column('data_source_city', sa.Text),
        sa.Column('notes', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_receipt',
        sa.Column('receipt_id', sa.Integer, primary_key=True),
        sa.Column('request_id', sa.Integer),
        sa.Column('data_source_id', sa.Integer),
        sa.Column('date_received', sa.TIMESTAMP),
        sa.Column('received_by', sa.Text),
        sa.Column('transfer_method', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_set',
        sa.Column('data_set_id', sa.Integer, primary_key=True),
        sa.Column('title', sa.Text),
        sa.Column('description', sa.Text),
        sa.Column('document_link', sa.Text),
        sa.Column('keywords', sa.Text),
        sa.Column('category', sa.Text),
        sa.Column('data_source_id', sa.Integer),
        sa.Column('data_set_contact', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_collection',
        sa.Column('data_collection_id', sa.Integer, primary_key=True),
        sa.Column('title', sa.Text),
        sa.Column('description', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_collection_member',
        sa.Column('data_collection_id', sa.Integer),
        sa.Column('data_set_id', sa.Integer),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_set_request',
        sa.Column('request_id', sa.Integer),
        sa.Column('data_set_id', sa.Integer),
        sa.Column('data_start_date', sa.TIMESTAMP),
        sa.Column('data_end_date', sa.TIMESTAMP),
        sa.Column('notes', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    status_type = sa.Enum(  # Type used for validation status below.
        'pending',
        'rejected',
        'passed',
        'force_pass',
        'exempt',
        name='status'
    )

    op.create_table(
        'data_table',
        sa.Column('data_table_id', sa.Integer, primary_key=True),
        sa.Column('receipt_id', sa.Integer),
        sa.Column('request_id', sa.Integer),
        sa.Column('data_set_id', sa.Integer),
        sa.Column('contact', sa.TEXT),
        sa.Column('date_received', sa.TIMESTAMP),
        sa.Column('stage', sa.Text),
        sa.Column('expected_start_date', sa.TIMESTAMP),
        sa.Column('expected_end_date', sa.TIMESTAMP),
        sa.Column('start_date', sa.TIMESTAMP),
        sa.Column('end_date', sa.TIMESTAMP),
        sa.Column('number_rows', sa.Integer),
        sa.Column('number_columns', sa.Integer),
        sa.Column('notes', sa.Text),
        sa.Column('server_location', sa.Text),
        sa.Column('path', sa.Text),
        sa.Column('file_table_name', sa.Text),
        sa.Column('format', sa.Text),
        sa.Column('size', sa.Numeric),
        sa.Column('validation_status', status_type),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'etl_input',
        sa.Column('workflow_id', sa.Integer),
        sa.Column('etl_step', sa.Integer),
        sa.Column('data_table_id', sa.Integer),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'etl_step_run',
        sa.Column('workflow_id', sa.Integer),
        sa.Column('etl_step', sa.Integer),
        sa.Column('run_date', sa.TIMESTAMP),
        sa.Column('run_by', sa.Text),
        sa.Column('script_path', sa.Text),
        sa.Column('commit', sa.Text),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'etl_output',
        sa.Column('workflow_id', sa.Integer),
        sa.Column('etl_step', sa.Integer),
        sa.Column('data_table_id', sa.Integer),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'column_info',
        sa.Column('column_id', sa.Integer, primary_key=True),
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('data_type', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'numeric_column',
        sa.Column('column_id', sa.Integer, primary_key=True),
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('minimum', sa.Numeric),
        sa.Column('maximum', sa.Numeric),
        sa.Column('mean', sa.Numeric),
        sa.Column('median', sa.Numeric),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'text_column',
        sa.Column('column_id', sa.Integer, primary_key=True),
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('max_length', sa.Integer),
        sa.Column('min_length', sa.Integer),
        sa.Column('median_length', sa.Integer),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'date_column',
        sa.Column('column_id', sa.Integer, primary_key=True),
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('max_date', sa.Date),
        sa.Column('min_date', sa.Date),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'code_frequency',
        sa.Column('code_id', sa.Integer, primary_key=True),
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('code', sa.Text),
        sa.Column('frequency', sa.Integer),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'data_dictionary',
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('columns_description', sa.Text),
        sa.Column('data_type', sa.Text),
        sa.Column('description', sa.Text),
        sa.Column('alternate_names', sa.Text),
        sa.Column('notes', sa.Text),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'numeric_range',
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('minimum', sa.Integer),
        sa.Column('minimum_tolerance', sa.Integer),
        sa.Column('maximum', sa.Integer),
        sa.Column('maximum_tolerance', sa.Integer),
        sa.Column('mean', sa.Integer),
        sa.Column('mean_tolerance', sa.Integer),
        sa.Column('median', sa.Integer),
        sa.Column('median_tolerance', sa.Integer),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'text_range',
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('max_length', sa.Integer),
        sa.Column('max_length_tolerance', sa.Integer),
        sa.Column('min_length', sa.Integer),
        sa.Column('min_length_tolerance', sa.Integer),
        sa.Column('median_length', sa.Integer),
        sa.Column('median_length_tolerance', sa.Integer),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'date_range',
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('max_date', sa.DATE),
        sa.Column('max_date_range', sa.Interval),
        sa.Column('min_date', sa.DATE),
        sa.Column('min_date_range', sa.Interval),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    op.create_table(
        'codebook',
        sa.Column('data_table_id', sa.Integer),
        sa.Column('column_name', sa.Text),
        sa.Column('code', sa.Text),
        sa.Column('description', sa.Text),
        sa.Column('expected_frequency', sa.Integer),
        sa.Column('expected_frequency_interval', sa.Integer),
        sa.Column('created_by', sa.Text),
        sa.Column('date_created', sa.TIMESTAMP),
        sa.Column('updated_by', sa.Text),
        sa.Column('date_last_updated', sa.TIMESTAMP),
        schema=SCHEMA_NAME
    )

    # Create foreign keys on data_request.
    op.create_foreign_key(
        'data_request_data_source_fk',
        'data_request',
        'data_source',
        ['data_source_id'],
        ['data_source_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create foreign keys on contact_history.
    op.create_foreign_key(
        'contact_history_data_request_fk',
        'contact_history',
        'data_request',
        ['request_id'],
        ['request_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create foreign keys on data_set_request.
    op.create_foreign_key(
        'data_set_request_data_request_fk',
        'data_set_request',
        'data_request',
        ['request_id'],
        ['request_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'data_set_request_data_set_fk',
        'data_set_request',
        'data_set',
        ['data_set_id'],
        ['data_set_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create composite key on data_set_request
    op.create_primary_key(
        'data_set_request_pk',
        'data_set_request',
        ['request_id', 'data_set_id'],
        schema=SCHEMA_NAME,
    )

    # Create foreign keys on data_set.
    op.create_foreign_key(
        'data_set_data_source_fk',
        'data_set',
        'data_source',
        ['data_source_id'],
        ['data_source_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create composite key on data_collection_member.
    op.create_primary_key(
        'data_collection_member_pk',
        'data_collection_member',
        ['data_collection_id', 'data_set_id'],
        schema=SCHEMA_NAME,
    )

    # Create foreign keys on data_collection_member.
    op.create_foreign_key(
        'data_collection_member_data_collection_fk',
        'data_collection_member',
        'data_collection',
        ['data_collection_id'],
        ['data_collection_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'data_collection_member_data_set_fk',
        'data_collection_member',
        'data_set',
        ['data_set_id'],
        ['data_set_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create foreign keys on data_receipt.
    op.create_foreign_key(
        'data_receipt_data_request_fk',
        'data_receipt',
        'data_request',
        ['request_id'],
        ['request_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'data_receipt_data_source_fk',
        'data_receipt',
        'data_source',
        ['data_source_id'],
        ['data_source_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create foreign keys on data_table.
    op.create_foreign_key(
        'data_table_data_receipt_fk',
        'data_table',
        'data_receipt',
        ['receipt_id'],
        ['receipt_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'data_table_data_request_fk',
        'data_table',
        'data_request',
        ['request_id'],
        ['request_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'data_table_data_set_fk',
        'data_table',
        'data_set',
        ['data_set_id'],
        ['data_set_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on etl_input.
    op.create_primary_key(
        'etl_input_pk',
        'etl_input',
        ['workflow_id', 'etl_step', 'data_table_id'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'etl_input_data_table_fk',
        'etl_input',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create composite key on etl_step_run.
    op.create_primary_key(
        'etl_step_run_pk',
        'etl_step_run',
        ['workflow_id', 'etl_step', 'run_date'],
        schema=SCHEMA_NAME,
    )

    # Create keys on etl_output.
    op.create_primary_key(
        'etl_output_pk',
        'etl_output',
        ['workflow_id', 'etl_step', 'data_table_id'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'etl_output_data_table_fk',
        'etl_output',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on column_info.
    op.create_foreign_key(
        'column_info_data_table_fk',
        'column_info',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on numeric_column.
    op.create_foreign_key(
        'numeric_column_column_info_fk',
        'numeric_column',
        'column_info',
        ['column_id'],
        ['column_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'numeric_column_data_table_fk',
        'numeric_column',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on text_columns.
    op.create_foreign_key(
        'text_column_column_info_fk',
        'text_column',
        'column_info',
        ['column_id'],
        ['column_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'text_column_data_table_fk',
        'text_column',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on date_columns.
    op.create_foreign_key(
        'date_column_column_info_fk',
        'date_column',
        'column_info',
        ['column_id'],
        ['column_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'date_column_data_table_fk',
        'date_column',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on code_frequency.
    op.create_foreign_key(
        'code_frequency_column_info_fk',
        'code_frequency',
        'column_info',
        ['column_id'],
        ['column_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'code_frequency_data_table_fk',
        'code_frequency',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on data_dictionary.
    op.create_primary_key(
        'data_dictionary_pk',
        'data_dictionary',
        ['data_table_id', 'column_name'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'data_dictionary_data_data_fk',
        'data_dictionary',
        'data_table',
        ['data_table_id'],
        ['data_table_id'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on numeric_range.
    op.create_primary_key(
        'numeric_range_pk',
        'numeric_range',
        ['data_table_id', 'column_name'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'numeric_range_data_dictionary_fk',
        'numeric_range',
        'data_dictionary',
        ['data_table_id', 'column_name'],
        ['data_table_id', 'column_name'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on text range.
    op.create_primary_key(
        'text_range_pk',
        'text_range',
        ['data_table_id', 'column_name'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'text_range_data_dictionary_fk',
        'text_range',
        'data_dictionary',
        ['data_table_id', 'column_name'],
        ['data_table_id', 'column_name'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on date range.
    op.create_primary_key(
        'date_range_pk',
        'date_range',
        ['data_table_id', 'column_name'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'date_range_data_dictionary_pk',
        'date_range',
        'data_dictionary',
        ['data_table_id', 'column_name'],
        ['data_table_id', 'column_name'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )

    # Create keys on codebook.
    op.create_primary_key(
        'codebook_pk',
        'codebook',
        ['data_table_id', 'column_name'],
        schema=SCHEMA_NAME,
    )

    op.create_foreign_key(
        'codebook_data_dictionary_fk',
        'codebook',
        'data_dictionary',
        ['data_table_id', 'column_name'],
        ['data_table_id', 'column_name'],
        source_schema=SCHEMA_NAME,
        referent_schema=SCHEMA_NAME,
    )


def downgrade():
    '''Drop tables and associated constraints and types.'''

    op.drop_constraint(
        'data_request_data_source_fk',
        'data_request',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'contact_history_data_request_fk',
        'contact_history',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_set_request_data_request_fk',
        'data_set_request',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_set_request_data_set_fk',
        'data_set_request',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_set_data_source_fk',
        'data_set',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_collection_member_data_collection_fk',
        'data_collection_member',
        schema=SCHEMA_NAME
    )

    op.drop_constraint(
        'data_collection_member_data_set_fk',
        'data_collection_member',
        schema=SCHEMA_NAME
    )

    op.drop_constraint(
        'data_receipt_data_request_fk',
        'data_receipt',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_receipt_data_source_fk',
        'data_receipt',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_table_data_receipt_fk',
        'data_table',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_table_data_request_fk',
        'data_table',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_table_data_set_fk',
        'data_table',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'etl_input_data_table_fk',
        'etl_input',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'etl_output_data_table_fk',
        'etl_output',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'column_info_data_table_fk',
        'column_info',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'numeric_column_data_table_fk',
        'numeric_column',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'text_column_data_table_fk',
        'text_column',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'date_column_data_table_fk',
        'date_column',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'code_frequency_data_table_fk',
        'code_frequency',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'numeric_column_column_info_fk',
        'numeric_column',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'text_column_column_info_fk',
        'text_column',
        schema=SCHEMA_NAME
    )

    op.drop_constraint(
        'date_column_column_info_fk',
        'date_column',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'code_frequency_column_info_fk',
        'code_frequency',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'data_dictionary_data_data_fk',
        'data_dictionary',
        schema=SCHEMA_NAME
    )

    op.drop_constraint(
        'numeric_range_data_dictionary_fk',
        'numeric_range',
        schema=SCHEMA_NAME,
    )

    op.drop_constraint(
        'text_range_data_dictionary_fk',
        'text_range',
        schema=SCHEMA_NAME
    )

    op.drop_constraint(
        'date_range_data_dictionary_pk',
        'date_range',
        schema=SCHEMA_NAME
    )

    op.drop_constraint(
        'codebook_data_dictionary_fk',
        'codebook',
        schema=SCHEMA_NAME,
    )

    op.drop_table('data_request', schema=SCHEMA_NAME)
    op.drop_table('contact_history', schema=SCHEMA_NAME)
    op.drop_table('data_source', schema=SCHEMA_NAME)
    op.drop_table('data_receipt', schema=SCHEMA_NAME)
    op.drop_table('data_set', schema=SCHEMA_NAME)
    op.drop_table('data_collection', schema=SCHEMA_NAME)
    op.drop_table('data_collection_member', schema=SCHEMA_NAME)
    op.drop_table('data_set_request', schema=SCHEMA_NAME)
    op.drop_table('data_table', schema=SCHEMA_NAME)
    op.drop_table('etl_input', schema=SCHEMA_NAME)
    op.drop_table('etl_step_run', schema=SCHEMA_NAME)
    op.drop_table('etl_output', schema=SCHEMA_NAME)
    op.drop_table('column_info', schema=SCHEMA_NAME)
    op.drop_table('numeric_column', schema=SCHEMA_NAME)
    op.drop_table('text_column', schema=SCHEMA_NAME)
    op.drop_table('date_column', schema=SCHEMA_NAME)
    op.drop_table('code_frequency', schema=SCHEMA_NAME)
    op.drop_table('data_dictionary', schema=SCHEMA_NAME)
    op.drop_table('numeric_range', schema=SCHEMA_NAME)
    op.drop_table('text_range', schema=SCHEMA_NAME)
    op.drop_table('date_range', schema=SCHEMA_NAME)
    op.drop_table('codebook', schema=SCHEMA_NAME)

    op.execute('drop type status')
