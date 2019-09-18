from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift,
                 aws_credentials,
                 table="",
                 s3_bucket="",
                 s3_path="",
                 json_path=None,
                 *args,
                 **kwargs):
        """
        Initialize the StageToRedshiftOperator

        :param redshift: Postgres connection to redshift
        :param aws_credentials:  AWS credentials object
        :param table: The table to insert data into
        :param s3_bucket: The s3 bucket the json files are located
        :param s3_path: The s3 path to load json files
        :param json_path: The location of the json file defining schema
        :param args: Additional args
        :param kwargs: Additional keyword args
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.credentials = aws_credentials
        self.redshift = redshift
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.json_path = json_path

    def execute(self, context):
        """
        Load data from s3 into a staging table in redshift

        :param context: The execution context
        :return:
        """
        if self.json_path is None:
            path = "auto"
        else:
            path = self.json_path

        s3 = f"s3://{self.s3_bucket}{self.s3_path}"
        sql = f"COPY {self.table} FROM '{s3}' ACCESS_KEY_ID '{self.credentials.access_key}' " \
              f"SECRET_ACCESS_KEY '{self.credentials.secret_key}' JSON '{path}'"
        self.log.info(f"{sql}")
        self.redshift.run(sql)

