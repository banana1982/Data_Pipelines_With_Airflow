from airflow.contrib.hooks.aws_hook import AwsHook # type: ignore
from airflow.hooks.postgres_hook import PostgresHook # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_create_table=None,
                 sql_statement=None,
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_create_table = sql_create_table
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.sql_create_table:
            self.log.info(f'Creating {self.table} in Redshift')
            redshift.run(self.sql_create_table.format())
            self.log.info(f'Creating table {self.table} in Redshift successfully')
                
        # self.log.info("Clearing data from destination Redshift table")
        # redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        INSERT_SQL = """
            INSERT INTO {} ( {} )
        """
        formatted_sql = INSERT_SQL.format(self.table, self.sql_statement)
        self.log.info(formatted_sql)
        redshift.run(formatted_sql)
        self.log.info('Copying data from S3 to Redshift successfully!')

        self.log.info('Done LoadFactOperator')
