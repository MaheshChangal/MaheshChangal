#Packages Used in the file
import emrlib.EMR_Plugin as emr
import os
import json
import random
import requests
import boto3
import io
import re
import pandas as pd
from datetime import date,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.operators.http_operator import SimpleHttpOperator

#default params
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}


# Initialize the DAG
dag = DAG('transform_data', concurrency=3, schedule_interval=None, default_args=default_args)
s3 = boto3.resource('s3')
region = 'us-east-1'

# Creates an EMR cluster
def create_emr(**kwargs):
    ti = kwargs['ti']
    conf = ti.xcom_pull(key='data')
    region = conf['region']
    cluster_id = emr.create_cluster(region_name=region, cluster_name='mahesh_cluster', num_core_nodes=2)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)

# fetchConfig is used to get app_config file from s3 bucket   
def fetchConfig(**kwargs):
    ti = kwargs['ti']
    conf = ti.xcom_pull(key="data")
    bucket = conf["app_config_bucket"]
    path = conf['app_config_path']
    dataset_name = conf['dataset_name']
    
    obj = s3.Object(bucket, path)
    body = obj.get()['Body'].read()
    confData = json.loads(body)
    
    s3_bucket_locs = dict()
    s3_bucket_locs['landing_bucket'] = confData['landing-bucket']
    s3_bucket_locs['raw_bucket'] = confData['raw-bucket']
    s3_bucket_locs['staging_bucket'] = confData['staging-bucket']
    s3_bucket_locs['datatype_update_cols'] = confData['mask-'+dataset_name]['datatype-update-cols']
    
    s3_bucket_locs['dataset_source'] = confData['ingest-'+dataset_name]['source']['data-location']
    s3_bucket_locs['dataset_dest'] = confData['ingest-'+dataset_name]['destination']['data-location']
    
    ti.xcom_push(key="S3_LandingBucket_locations",value=s3_bucket_locs)

#landing zone to raw zone
def landing_to_raw(**kwargs):
    ti = kwargs['ti']
    conf = ti.xcom_pull(key="data")
    locations = ti.xcom_pull(key='S3_LandingBucket_locations')
    dataset_name = conf['dataset_name']
    
    my_bucket = s3.Bucket(locations['raw_bucket'])
    
    copy_source = {
          'Bucket': conf['bucket'],
          'Key': conf['key']
        }
    
    bucket = s3.Bucket(locations['raw_bucket'])
    bucket.copy(copy_source, conf['key'])
    
# Transformations on datasets through livy submit
def transformations(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    conf = ti.xcom_pull(key="data")
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, conf["dataset_name"],conf["code_path"],conf["spark_config_path"],conf['key'],conf['landing_bucket'],conf["env"])
    emr.track_statement_progress(cluster_dns,headers)
    
def get_response(**kwargs):
    ti = kwargs['ti']
    data = dict()
    for key, value in kwargs["params"].items():
        data[key] = value

    ti.xcom_push(key="data",value=data)
    
def preValidation(**kwargs):
    ti = kwargs['ti']
    conf = ti.xcom_pull(key="data")
    locations = ti.xcom_pull(key='S3_LandingBucket_locations')
    bucket = locations['raw_bucket']
    path = conf['key']
    pre_validation_data = dict()
    print(path)
    print('/'.join(path.split('/')[:-1]))
    
    my_bucket = s3.Bucket(bucket)
    
    for obj in my_bucket.objects.filter(Prefix='/'.join(path.split('/')[:-1])+"/"):
        
        if obj.key.strip().split("/")[-1]=="":
            continue
        buffer = io.BytesIO()
        object = s3.Object(bucket,obj.key)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
    
        if df.count()[0]==0:
            raise Exception("Data Validation Exception : No Data found in the dataset")
       
        #Adding count of df into the data dictoniary
        pre_validation_data['counts'] = str(int(pre_validation_data.get('counts','0'))+df.count()[0]) 
    
    ti.xcom_push(key="pre_validation_data",value=pre_validation_data)
    
def postValidation(**kwargs):
    ti = kwargs['ti']
    conf = ti.xcom_pull(key="data")
    locations = ti.xcom_pull(key='S3_LandingBucket_locations')
    dataset = conf['dataset_name']
    raw = locations["raw_bucket"]
    stage = locations['staging_bucket']
    day = date.today()
    
    today = date.today()
    months = ['January','February','March','April','May','June','July','August','September','October','November','December']
    month = months[today.month-1]
    
    my_bucket = s3.Bucket(stage)     
    path = 'datasets/'+dataset+'/'+'month='+month+'/'+'date='+str(day)+'/'
  
    datatype_update_cols = locations['datatype_update_cols']
    pre = ti.xcom_pull(key='pre_validation_data')
    pre_count = int(pre['counts'])
    print(path)
    
    cols = []
    for item in datatype_update_cols:
        col_name = item.split(":")[0]
        datatype = item.split(":")[-1]
        
        dtype = re.findall("([\w]*)(?:Type)",datatype)[0].lower()
        if dtype=='string':
            cols.append((col_name,str))
        elif dtype=='integer':
            cols.append((col_name,int))
        elif dtype=='double':
            cols.append((col,float))
            
    post_count = 0

    for obj in my_bucket.objects.filter(Prefix=path):
        print(obj.key)
        buffer = io.BytesIO()
        object = s3.Object(stage,obj.key)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
    
        post_count += df.count()[0]
        df.columns = [i.lower() for i in df.columns]
    
        for item in cols:
            if type(df[item[0]][0])==item[1]:
                continue
            else:
                raise Exception("Validation Error! Datatype mismatch found.")
        
    print(post_count)
    print(pre_count)
    if pre_count!=post_count:
        raise Exception("Validation Error! Rows mismatch found.")
        
            
            
            
get_app_config = PythonOperator(
    task_id = "Fetching_App_Config",
    python_callable = fetchConfig,
    dag = dag)

landing_to_raw = PythonOperator(
    task_id = 'Landing_Zone_to_Raw_Zone_',
    python_callable = landing_to_raw,
    dag=dag)

create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)



terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

get_data = PythonOperator(
    task_id='get_data',
    python_callable=get_response,
    dag=dag
    )

pre_validation = PythonOperator(
    task_id='pre_validation',
    python_callable=preValidation,
    dag=dag
    )

transform_data = PythonOperator(
    task_id='transformations',
    python_callable=transformations,
    dag=dag)

post_validation = PythonOperator(
    task_id='post_validation',
    python_callable=postValidation,
    dag=dag
    )


get_data >> get_app_config >> landing_to_raw >> pre_validation >> create_cluster >> wait_for_cluster_completion >> transform_data >> post_validation  >> terminate_cluster
