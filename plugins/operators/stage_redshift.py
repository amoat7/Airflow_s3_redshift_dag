from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    

    @apply_defaults
    def __init__(self,
                 redshift_id = "",
                 aws_credentials= "",
                 table ="",
                 s3_bucket = "",
                 s3_key="",
                 region = "",
                 json = "",
                 ignore_headers ="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_id = redshift_id 
        self.aws_cred = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json = json
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_cred)
        cred = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_id)
        self.log.info(f"Clearing data from {self.table}")
        redshift.run("DELETE FROM {}".format(self.table)) 
        
        self.log.info(f"Copying data from S3 to {self.table}")
        s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{s3_key}"
        
        sql_copy = f"""
        COPY {self.table}
        FROM '{s3_path}'
        region '{self.region}'
        ACCESS_KEY_ID '{cred.access_key}'
        SECRET_ACCESS_KEY '{cred.secret_key}'
        json '{self.json}'
        IGNOREHEADER {self.ignore_headers};
        """
        redshift.run(sql_copy)
        
        





