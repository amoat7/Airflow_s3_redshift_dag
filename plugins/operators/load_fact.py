from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_id = "",
                 table = "",
                 query ="",
                 truncate = True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_id = redshift_id
        self.query=query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(self.redshift_id)
        if self.truncate:
            self.log.info(f"Clearing data from {self.table}")
            redshift.run("DELETE FROM {}".format(self.table)) 
        self.log.info(f'Running query')
        redshift.run(f'Insert into {self.table}{self.query}')