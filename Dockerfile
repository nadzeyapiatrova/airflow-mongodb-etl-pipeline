FROM apache/airflow:2.9.2
RUN pip install --no-cache-dir "apache-airflow==2.9.2" pandas "apache-airflow-providers-mongo==4.1.0" pymongo