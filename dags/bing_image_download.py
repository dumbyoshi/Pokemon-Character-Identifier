# -*- coding: utf-8 -*-
"""
Created on Sat Dec  4 12:00:43 2021

@author: Administrator
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import tensorflow
import PIL
from minio import Minio
from PIL import ImageFile
from minio.commonconfig import REPLACE, CopySource
from tensorflow.keras.applications import imagenet_utils
from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications.imagenet_utils import preprocess_input, decode_predictions
import numpy as np
import requests
from PIL import ImageFile
import cv2
import os
import shutil
import boto3

#process_dir=os.path.join(os.getcwd(),"dags/")
# ========================create a process directory===================================
process_dir=os.path.join(os.getcwd(),"dags/")

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
found = client.bucket_exists("sourcedata")
if not found:
    client.make_bucket("sourcedata")

found2 = client.bucket_exists("destinationdata")
if not found2:
    client.make_bucket("destinationdata")
client.fput_object(
    "destinationdata", "pokemon.h5", process_dir+"/pokemon.h5",
)
# ================================custom functions=============================

def bing_image_download():
    # set your Microsoft Cognitive Services API key along with (1) the
    # maximum number of results for a given search and (2) the group size
    # for results (maximum of 50 per request)
    API_KEY = "120b513963534d4bac4d968105b5c44d"
    folder = process_dir+'images/'
    MAX_RESULTS = 200
    GROUP_SIZE = 50
    
    if not os.path.isdir(folder):
        os.mkdir(folder)
    
    # set the endpoint API URL
    URL = "https://api.bing.microsoft.com/v7.0/images/search/"
    
    # store the search term in a convenience variable then set the
    pokemons = ["pikachu","bulbasaur","charmander"]
    
    
    for term in pokemons:
        # headers and search parameters
        headers = {"Ocp-Apim-Subscription-Key" : API_KEY}
        params = {"q": term, "offset": 0, "count": GROUP_SIZE}
        
        # make the search
        print("[INFO] searching Bing API for '{}'".format(term))
        search = requests.get(URL, headers=headers, params=params)
        search.raise_for_status()
        
        # grab the results from the search, including the total number of
        # estimated results returned by the Bing API
        results = search.json()
        estNumResults = min(results["totalEstimatedMatches"], MAX_RESULTS)
        print("[INFO] {} total results for '{}'".format(estNumResults,
        	term))
         
        # initialize the total number of images downloaded thus far
        total = 0
        for i in range(0,1):
            # loop over the estimated number of results in `GROUP_SIZE` groups
            for offset in range(i, estNumResults, GROUP_SIZE):
                # update the search parameters using the current offset, then
                # make the request to fetch the results
                print("[INFO] making request for group {}-{} of {}...".format(offset, offset + GROUP_SIZE, estNumResults))
                params["offset"] = offset
                search = requests.get(URL, headers=headers, params=params)
                search.raise_for_status()
                results = search.json()
                print("[INFO] saving images for group {}-{} of {}...".format(offset, offset + GROUP_SIZE, estNumResults))
                
                # loop over the results
                for v in results["value"]:
                    # try to download the image
                    try:
                        # make a request to download the image
                        print("[INFO] fetching: {}".format(v["contentUrl"]))            
                        r = requests.get(v["contentUrl"], timeout=30)
                         
                        # build the path to the output image
                        ext = v["contentUrl"][v["contentUrl"].rfind("."):]
                        img_type = v["encodingFormat"]
                        # p = os.path.sep.join([args["output"], "{}{}".format(str(total).zfill(8), ext)])
                        #p = os.path.sep.join([folder, "{}_{}.{}".format(term,str(total), img_type)])
                        p = folder+"{}_{}.{}".format(term,str(total), img_type)
                        # p = p.replace("\\","/")
                        
                        if str(img_type) != "animatedgif":
                        # write the image to disk
                            f = open(p, "wb")
                            f.write(r.content)
                            f.close()
                            total += 1
                            
                            # try to load the image from disk
                            image = cv2.imread(p)
                
                            # if the image is `None` then we could not properly load the
                            # image from disk (so it should be ignored)
                            if image is None:
                                    print("[INFO-DEL] deleting: {}".format(p))
                                    os.remove(p)
                                    continue
                            else:
                                client.fput_object(
                                source_bucket, p.split('/')[-1], p)
                                os.remove(p)
                            
             
                    # catch any errors that would not unable us to download the
                    # image
                    except Exception as e:
                        # check to see if our exception is in our list of
                        # exceptions to check for
                        print("[ERROR]: ",str(e))
        
                    
        print("*"*50)            
        print("[END] Images Donwloaded for",term)
        print("*"*50)
 

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

dag=DAG('bing_image_download',
         start_date=datetime(2021, 12, 1),
         max_active_runs=2,
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         catchup=False
         ) 

start_= DummyOperator(
        task_id='start',
        dag=dag,
        )


bing_image_download=PythonOperator(
    task_id ='bing_image_download',
    python_callable=bing_image_download,
    dag=dag
         )


end_dummy=DummyOperator(
    task_id='end',
    dag=dag,
         )

start_ >> bing_image_download >> end_dummy

