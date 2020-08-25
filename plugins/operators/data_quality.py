from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_id = "",
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_id = redshift_id
        self.checks = checks

    def execute(self, context):
        self.log.info('Data quality checks running')
        redshift = PostgresHook(self.redshift_id)
        
        for check in self.checks:
            sql_check = check["sql_check"]
            res = check["expected"]
            get_res = redshift.get_records(sql_check)
            num_ = get_res[0][0]
            if num_ != res:
                raise ValueError("Data Quality Check failed. Expected {res} but received {num_}")
            else:
                self.log.info(f" Data quality Check passed ")