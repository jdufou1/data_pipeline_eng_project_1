FROM python:3.10.12
# supervisord setup                       
RUN apt-get update && apt-get install -y supervisor                 
# Install libffi-dev
RUN apt-get update && apt-get install -y libffi-dev      
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
# Airflow setup                       
ENV AIRFLOW_HOME=/app/airflow
# Set the DAGs folder
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/airflow/dags

RUN pip install --upgrade pip
COPY requirements.txt $AIRFLOW_HOME/
RUN pip install -r $AIRFLOW_HOME/requirements.txt
RUN pip install apache-airflow        


COPY /credentials/ $AIRFLOW_HOME/credentials
COPY /dags/dag_pipeline.py $AIRFLOW_HOME/dags/
COPY /dags/etl_functions.py $AIRFLOW_HOME/dags/

RUN airflow db init

RUN airflow users create \
    --username jdufou1 \
    --firstname jeremy \
    --lastname dufourmantelle \
    --role Admin \
    --email jeremy.dufourmantelle@gmail.com \
    --password jyde-7819020

# Activez automatiquement le DAG après la création de la base de données
# RUN airflow dags trigger dag_pipeline_v1

RUN sed -i 's/load_examples = True/load_examples = False/' $AIRFLOW_HOME/airflow.cfg

EXPOSE 8080
CMD ["bash", "-c", "airflow webserver -p 8080 & airflow scheduler && wait"]
