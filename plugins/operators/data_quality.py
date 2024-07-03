
import logging
from airflow.hooks.postgres_hook import PostgresHook # type: ignore
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 list_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_checks = list_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.list_checks:
             for item_check in enumerate(self.list_checks):
                self.log.info(f"Performing data quality check with query: {item_check}")
                
                records = redshift.get_records(item_check)
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {item_check} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {item_check} contained 0 rows")
                logging.info(f"Data quality on table {item_check} check passed with {records[0][0]} records")
                self.log.info('DataQualityOperator not implemented yet')
