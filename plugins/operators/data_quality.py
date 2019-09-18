from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift,
                 sql_select,
                 expected_result,
                 *args, **kwargs):
        """
        Initialize the DataQualityOperator

        :param redshift: Postgres connection to redshift
        :param sql_select: The sql statement to get results for validation
        :param expected_result: The expected results of sql_select parameter
        :param args: Additional args
        :param kwargs: Additional keyword args
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.sql_select = sql_select
        self.expected_result = expected_result

    def execute(self, context):
        """
        Test the result of a sql query against some expected result

        :param context: The execution context
        :return: None
        """
        self.log.info(f"Validate data using query: {self.sql_select}")
        result = self.redshift.get_records(self.sql_select)
        assert (self.expected_result == result), f'Expected result is different than actual result ' \
                                                 f'{self.expected_result} != {result}'
        self.log.info(f"Validation Successful!")
