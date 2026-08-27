from airflow import DAG
from datetime import timedelta
from datetime import datetime, date
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import dag, Label, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from dotenv import load_dotenv
import requests
import logging
import pickle
import pandas as pd
from io import BytesIO
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

load_dotenv()
logger = logging.getLogger("__name__")

default_args={
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function, # or list of functions
    # 'on_success_callback': some_other_function, # or list of functions
    # 'on_retry_callback': another_function, # or list of functions
    # 'sla_miss_callback': yet_another_function, # or list of functions
    # 'on_skipped_callback': another_function, #or list of functions
    # 'trigger_rule': 'all_success'
}

dates = datetime.now().strftime("%Y-%m-%d")


@dag("palu-jaya-pipeline",
    default_args = default_args,
    description="Palu Jaya ETL Pipeline",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 8, 6),
    catchup=False)

def palu_jaya_etl():
    @task(task_id="authentication")
    def authentication():
        pkl_cookies = None
        try:
            with open("cookies.pkl", "rb") as f:
                pkl_cookies = pickle.load(f)
        except FileNotFoundError:
            pass
        
        
        index_path = os.getenv("INDEX_PATH")
        login_path = os.getenv("LOGIN_PATH")
    
        
        data = {
            "LoginForm[username]": os.getenv("POS_USERNAME"),
            "LoginForm[password]": os.getenv("POS_PASSWORD"),
        }
        
        res = requests.post(index_path, cookies=pkl_cookies)
        res_content =res.content.decode()
        
        if "<!DOCTYPE html>" in res_content:
            try:
                session = requests.session()
                session.post(login_path, data=data)
                
                cks = session.cookies.get_dict()
                with open("cookies.pkl", "wb") as f:
                    pickle.dump(cks, f)
                return cks
            except Exception as e:
                logger.error(e)
        return pkl_cookies
    
    
    def store(base_path, cookies, params, s3_prefix, file_name, file_type="csv", sep=","):
        try:
            
            file_byte = requests.get(url=base_path, cookies=cookies, params=params)
            bytes = file_byte.content
            
            if (file_type == "xlsx"):

                df = pd.read_excel(BytesIO(bytes), skiprows=1, skipfooter=8)
                for col in df.columns:
                    if "Unnamed" in str(col):
                        df[col] = df[col].astype('str')
            elif (file_type == "csv"):
                df = pd.read_csv(BytesIO(bytes), sep=sep)
                

            s3_class_instance = S3Hook(aws_conn_id="aws_s3_palu_jaya_data")
            s3_class_instance.load_bytes(
                bytes_data = df.to_parquet(index=False),

                key = "raw/"+ s3_prefix + file_name,
                bucket_name = "palu-jaya-data-storage-910162731301-ap-southeast-1-an",
                replace = True)
        except Exception as e:
            logger.error(e)
            raise Exception
        
    
    @task(task_id="konsumen")
    def konsumen(cookies:dict):
        params = {
            "tanggal_1" : dates,
            "tanggal_2" : dates
        }
        s3_prefix = "raw-konsumen/"
        file_name = f"data-konsumen-{dates}.parquet"
        base_path= os.getenv("KONSUMEN_PATH")
        store(base_path, cookies, params, s3_prefix, file_name, sep=";")

    
    @task(task_id="pkb_rupiah")
    def pkb_rupiah(cookies:dict):
        params = {
           "tanggal1":dates,
           "tanggal2":dates,
           "perintah_kerja_grup_id": None,
           "perintah_kerja_antrian_id": None,
           "event_id": None,
        }
        
        s3_prefix = "raw-pkb-rupiah/"
        file_name = f"data-pkb-rupiah-{dates}.parquet"
        base_path = os.getenv("PKB_RUPIAH_PATH")
        store(base_path, cookies, params, s3_prefix, file_name)
    
    @task(task_id = "njb")
    def njb(cookies: dict):
        params = {
           "tanggal1":dates,
           "tanggal2":dates,
           "type" : "xlsx"
        }
        
        base_path = os.getenv("NJB_PATH")
        s3_prefix = "raw-njb/"
        file_name = f"data-njb-{dates}.parquet"
        store(base_path, cookies, params, s3_prefix, file_name, file_type="xlsx")
            
    @task(task_id="nsc")
    def nsc(cookies: dict):
        params = {
           "tanggal1":dates,
           "tanggal2":dates,
           "type" : "xlsx"
        }
        base_path = os.getenv("NSC_PATH")
        s3_prefix = "raw-nsc/"
        file_name = f"data-nsc-{dates}.parquet"
        store(base_path, cookies, params, s3_prefix, file_name, file_type="xlsx")

    @task(task_id="remainder")
    def remainder(cookies:dict):
        params = {
            "durasi" : 60
        }
        
        base_path=os.getenv("REMAINDER_PATH")
        s3_prefix = "raw-remainder/"
        file_name = f"data-remainder-{dates}.parquet"
        store(base_path, cookies, params, s3_prefix, file_name, sep=";")

    @task_group
    def data_transformation():
        task_pkb_transformation = SparkSubmitOperator(
            task_id = "pkb_transformation",
            conn_id="spark_palu_jaya",
            application="/opt/notebooks/palu-jaya-transform/pkb.py",
        )
        
        task_remainder_transformation = SparkSubmitOperator(
            task_id = "remainder_transformation",
            conn_id="spark_palu_jaya",
            application="/opt/notebooks/palu-jaya-transform/remainder_konsumen_2_bulan.py",
        )
        
        task_nsc_transformation = SparkSubmitOperator(
            task_id = "nsc_transformation",
            conn_id="spark_palu_jaya",
            application="/opt/notebooks/palu-jaya-transform/nsc.py",
        )
        
        task_njb_transformation = SparkSubmitOperator(
            task_id = "njb_transformation",
            conn_id="spark_palu_jaya",
            application="/opt/notebooks/palu-jaya-transform/njb.py",
        )
        
        [task_pkb_transformation, task_remainder_transformation, task_nsc_transformation, task_njb_transformation]
    
    @task_group
    def data_ingestion(cookies):
      [konsumen(cookies), pkb_rupiah(cookies), njb(cookies), nsc(cookies), remainder(cookies)] 

    data_ingestion(authentication()) >> data_transformation()
    
palu_jaya_etl()

