from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries


class InitRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift,
                 aws_credentials,
                 *args,
                 **kwargs):
        """
        Initialize the InitRedshiftOperator

        :param redshift: Postgres connection to redshift
        :param aws_credentials:  AWS credentials object
        :param args: Additional args
        :param kwargs: Additional keyword args
        """
        super(InitRedshiftOperator, self).__init__(*args, **kwargs)
        self.credentials = aws_credentials
        self.redshift = redshift

    def execute(self, context):
        """
        (Re)Create all redshift tables used in the DAG
        All drop/create statement are defined in the SqlQueries helper

        :param context:  The execution context
        :return:
        """

        for sql in SqlQueries.drop_table_queries:
            self.log.info(f"{sql}")
            self.redshift.run(sql)

        for sql in SqlQueries.create_table_queries:
            self.log.info(f"{sql}")
            self.redshift.run(sql)


