from airflow.hooks.postgres_hook import PostgresHook # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement=None,
                 sql_create_table=None,
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.sql_create_table = sql_create_table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.sql_create_table:
            self.log.info(f'Creating {self.table} in Redshift')
            redshift.run(self.sql_create_table.format())
            self.log.info(f'Creating table {self.table} in Redshift successfully')

        if self.truncate:
            self.log.info("Truncate data from destination Redshift table")
            TRUNCATE_SQL = """
                TRUNCATE TABLE {}
            """
            formatted_sql = TRUNCATE_SQL.format(self.table)
            redshift.run(formatted_sql)

        self.log.info("Copying data from S3 to Redshift")
        redshift.run(self.sql_statement)
        self.log.info('LoadDimensionOperator not implemented yet')
