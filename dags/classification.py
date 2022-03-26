from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import tensorflow
from minio import Minio
from minio.commonconfig import REPLACE, CopySource
import os
from PIL import ImageFile
from tensorflow.keras.applications import imagenet_utils
from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.imagenet_utils import preprocess_input, decode_predictions
import numpy as np
import boto3
from PIL import ImageFile


process_dir=os.path.join(os.getcwd(),"dags/process_dir")
# ========================create a process directory===================================
def create_dir():
    if not os.path.exists(process_dir):
        os.mkdir(process_dir)

# ===================minio client=================================
endpoints=Variable.get("ENDPOINT_URL")
access_key=Variable.get("ACCESS_KEY")
secret_key=Variable.get("SECRET_KEY")
source_bucket=Variable.get("SOURCE_BUCKET")
destination_bucket=Variable.get("DESTINATION_BUCKET")

client = Minio(
    endpoint=endpoints,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

# =========================================
#sensing task

# ================================custom functions=============================

def is_minio_data_exist(**kwargs):
    """
    this function will check that data is exist or not in minio
    if exists then process further o/w end the process
    """
    images=[]
    objects = client.list_objects(source_bucket)
    for obj in objects:
        # apply filter if extension in ['.png','.jpg','.jpeg'] then append the file
        images.append(obj.object_name)
        print(obj.object_name)
    if len(images)>0:
        # https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html
        
        # set the filename in xcom variable so that we can process this data in another function
        ti = kwargs['ti']
        print("--------------ti-----------------------")
        print(ti)
        print("------------------------------")
        ti.xcom_push(key="images",value=images)
        return 'load_data'
    else:
        return 'end'

def download_file_for_processing(**kwargs):
    # check dir exist or nor
    create_dir()

    # first get list of all images that you have uploaded
    ti = kwargs['ti']
    images=ti.xcom_pull(key="images")

    # download the images for deep learning processing 
    for image in images:
        client.fget_object(
            source_bucket, image, os.path.join(process_dir,image),
        )
        #image = ti.xcom_push(key = "oneimage")
    
def delete_source_images(**kwargs):
    # remove source images from minio bucket
    ti = kwargs['ti']
    images=ti.xcom_pull(key="images")
    #=========================delete object=====================================
    # Remove list of objects.
    # ==============================================================================
    for image in images:
        delete_obj=client.remove_object(
            source_bucket,image,
        )

def apply_dl_model():

    client.fget_object(destination_bucket, 'pokemon.h5', "pokemon")
    fil = 'pokemon'
    model = tensorflow.keras.models.load_model(fil)
    
    model_labels=["pikachu_","bulbasaur_","charmander_"]
    
    #ti = kwargs['ti']
    #for pokemon_image in images:
    for i, filename in enumerate(os.listdir(process_dir)):
        img = image.load_img(os.path.join(process_dir,filename), target_size=(64, 64))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        preds = model.predict(x)
        result= np.argmax(preds)
        if(result==0):
            prediction_label = model_labels[0]
        elif(result==1):
            prediction_label = model_labels[1]
        else:
            prediction_label = model_labels[2]
        os.rename(process_dir+"/"+filename,process_dir+"/"+prediction_label +filename)

def save_result():
    clients3 = boto3.client('s3', region_name='ap-south-1', aws_access_key_id='AKIA6KVRCZIKSBAESUZW', aws_secret_access_key='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')
    print("hello")
    for i, filename in enumerate(os.listdir(process_dir)):
        predict_label,img=tuple(filename.split("_",1))
        result = clients3.upload_file(os.path.join(process_dir,filename), "pokemonimages8", predict_label+"/"+img)

        # remove this file from source dir
        #os.remove(process_dir+"/"+filename)

# ===============================================================================



# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=120)
}

dag=DAG('classification',
         start_date=datetime(2021, 12, 1),
         max_active_runs=2,
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         catchup=False
         ) 

start_ = DummyOperator(
        task_id='start',
        dag=dag,
        )

# Step 4 - Create a Branching task to Register the method in step 3 to the branching API


check_data_exist = BranchPythonOperator(
  task_id='check_data_exist',
  python_callable=is_minio_data_exist, #Registered method
  provide_context=True,
  dag=dag
)


load_data = PythonOperator(
    task_id = 'load_data',
    python_callable=download_file_for_processing,
    dag=dag
)

apply_model = PythonOperator(
    task_id = 'apply_model',
    python_callable=apply_dl_model,
    provide_context=True,
    dag=dag
)

delete_source_images = PythonOperator(
    task_id = 'delete_source_images',
    python_callable=delete_source_images,
    provide_context=True,
    dag=dag
)


save_result = PythonOperator(
    task_id = 'save_result',
    python_callable=save_result,
    dag=dag
)

end_dummy = DummyOperator(
    task_id='end',
    dag=dag,
    )

start_ >> check_data_exist>> load_data >> apply_model>> save_result >> delete_source_images >> end_dummy
#start_ >> save_result>> end_dummy
start_ >> check_data_exist>> end_dummy