from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift,
                 sql_select,
                 insert_table,
                 insert_mode,
                 *args, **kwargs):
        """

        :param redshift: Postgres connection to redshift
        :param sql_select: The select statement to populate the dim table with
        :param insert_table: The dim table to insert into
        :param insert_mode: The insert mode, delete-load will truncate the table first
        :param args: Additional args
        :param kwargs: Additional keyword args
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift
        self.sql_select = sql_select
        self.insert_table = insert_table
        self.insert_mode = insert_mode

    def execute(self, context):
        """
        Insert the results of a sql query into a table
        If insert_mode is `delete-load` the table will be truncated first.

        :param context:  The execution context
        :return:
        """
        if self.insert_mode == 'delete-load':
            self.redshift.run(f"TRUNCATE TABLE {self.insert_table}")
        sql = f'INSERT INTO {self.insert_table} ({self.sql_select})'
        self.log.info(f"{sql}")
        self.redshift.run(sql)
