from airflow import task

from airflow.contrib.hooks.aws_hook import AwsHook # type: ignore
from airflow.hooks.postgres_hook import PostgresHook # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_option="",
                 sql_statement=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        self.sql_statement = sql_statement

    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info(credentials.access_key)
        self.log.info(credentials.secret_key)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        if self.sql_statement:
            self.log.info(f'Creating {self.table} in Redshift')
            redshift.run(self.sql_statement)
            self.log.info(f'Creating table {self.table} in Redshift successfully')

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        s3_json_path = "auto"
        if self.json_option:
            s3_json_path = "s3://{}/{}".format(self.s3_bucket, self.json_option)

        formatted_sql = SqlQueries.COPY_SQL.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            s3_json_path,
            self.region
        )
        redshift.run(formatted_sql)
        self.log.info(
            f"StageToRedshiftOperator copy complete - {self.table}")