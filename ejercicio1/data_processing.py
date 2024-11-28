from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

spark = SparkSession.builder.appName("BancoBaseApp").getOrCreate()

def _clean_data(input_file, output_file):
    # Lista de estatus validos
    status_descriptions = ["refunded", "charged_back", "pre_authorized", "paid", "partially_refunded", "pending_payment", "expired", "voided"]
    # Carga del archivo a procesar
    data = spark.read.csv(input_file, header=True)
    # 1. Limpieza del campo estatus para valores inexistentes
    # 2. Homologacion de la longitud del campo created_at en formato 2021-10-21
    clean_data = data \
        .withColumn("status", when(data.status.isin(status_descriptions), data.status).otherwise(lit("not_description"))) \
        .withColumn("created_at",
            when(length(data.created_at) == 8,concat_ws("-", substring(data.created_at, 1, 4), substring(data.created_at, 5, 2), substring(data.created_at, 7, 2)))
            .when(length(data.created_at) > 10, substring(data.created_at, 1, 10))
            .otherwise(data.created_at))
    # Almacenamiento del dataframe en la capa de staging una vez limpios los datos
    clean_data.toPandas().to_csv(output_file, index=False)

def _process_data(input_file, output_file):
    # Carga del archivo a procesar
    data = spark.read.csv(input_file, header=True)
    processed_data = data.withColumn("amount", data.amount.cast("double")).groupBy("name", "created_at").sum("amount").alias("result")
    # Almacenamiento del dataframe con la informacion requerida en la capa de processed
    processed_data.toPandas().to_csv(output_file, index=False)

def _additional_data(input_file, output_file):
    # Carga del archivo a procesar
    data = spark.read.csv(input_file, header=True)
    # 1. Se filtra la informacion por el estatus paid
    # 2. Se agrega la columna difference_days que es la diferencia entre los campos created_at y paid_at
    # 3. Se agrega la informacion por company_id y se calcula la desviacion estandar de las empresas
    # 4. Se agrega un case when para etiquetar a las empresas dependiendo de su frecuencia de pago
    processed_data = data.filter(data.status == "paid").withColumn("difference_days", abs(datediff(data.created_at, data.paid_at)))
    processed_data = processed_data.groupBy("company_id").agg(stddev("difference_days").alias("stddev_days_to_pay"))
    processed_data = processed_data.withColumn("payment_pattern",
        when(processed_data.stddev_days_to_pay < 10, "Alta Regularidad")
        .when((processed_data.stddev_days_to_pay >= 10) & (processed_data.stddev_days_to_pay <= 30), "Moderada Regularidad")
        .when(processed_data.stddev_days_to_pay > 30, "Irregular").otherwise("Sin Descripcion"))
    # Almacenamiento del dataframe con la informacion requerida en la capa de processed
    processed_data.toPandas().to_csv(output_file, index=False)

with DAG(
        dag_id="data",
        start_date=datetime(2024, 10, 31),
        end_date=datetime(2024, 12, 31),
        schedule_interval=None,
        catchup=False,
) as dag:
    extract_data = EmptyOperator(
        task_id="extract_data",
    )

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=_clean_data,
        op_kwargs={
            "input_file": "/opt/airflow/data/raw/data_prueba_tecnica.csv",
            "output_file": "/opt/airflow/data/staging/data_prueba_tecnica.csv"
        }
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_data,
        op_kwargs={
            "input_file": "/opt/airflow/data/staging/data_prueba_tecnica.csv",
            "output_file": "/opt/airflow/data/processed/ejercicio1.csv"
        }
    )

    additional_data = PythonOperator(
        task_id="additional_data",
        python_callable=_additional_data,
        op_kwargs={
            "input_file": "/opt/airflow/data/staging/data_prueba_tecnica.csv",
            "output_file": "/opt/airflow/data/processed/ejercicio2.csv"
        }
    )

    extract_data >> clean_data >> [process_data, additional_data]
