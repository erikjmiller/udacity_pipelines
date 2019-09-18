from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift,
                 sql_select,
                 insert_table,
                 *args, **kwargs):
        """
        Initialize the LoadFactOperator

        :param redshift: Postgres connection to redshift
        :param sql_select: The select statement used to populate the fact table
        :param insert_table: The fact table to insert into
        :param args: Additional args
        :param kwargs: Additional keyword args
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.sql_select = sql_select
        self.insert_table = insert_table

    def execute(self, context):
        """
        Insert the results of sql query into a fact table

        :param context: The execution context
        :return:
        """
        sql = f'INSERT INTO {self.insert_table} ({self.sql_select})'
        self.log.info(f"{sql}")
        self.redshift.run(sql)
