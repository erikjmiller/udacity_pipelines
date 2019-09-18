FROM puckel/docker-airflow

USER root
RUN pip install apache-airflow[s3]
RUN pip install apache-airflow[postgres]

USER airflow
