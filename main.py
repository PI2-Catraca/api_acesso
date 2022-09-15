from datetime import date, datetime
from typing import List
from fastapi import FastAPI
import pickle
from pydantic import BaseModel
import schedule
import time
import os
from google.cloud import storage
from sqlalchemy import null
import json 
import psycopg2

app = FastAPI()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'aerial-velocity-359918-e385a21f34a1.json'
storage_client = storage.Client()
bucket_name_pickle = 'acesso-pi2'

def conecta_db():
  con = psycopg2.connect(host='localhost', port = '15432', database='db_catraca',user='postgres', password='password')
  return con

def dowload_file(blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with open(file_path, 'wb') as f:
            storage_client.download_blob_to_file(blob, f)
        print(bucket_name, datetime.now())
        return True
    except Exception as e:
        print(e)
        return False

def list_file(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    files = []

    for blob in blobs:
        files.append((blob_metadata(bucket_name, blob.name)))

    return files

def blob_metadata(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    return {'file': blob.name, 'bucket': blob.bucket.name, 'updated': blob.updated}

def compare_download_file(file1, file2):
    global acessoJson

    if file1 != null and file1['updated'] != file2['updated'] :
        dowload_file(file2['file'], os.path.join(os.getcwd(), './{nome}').format(nome = file2['file']), 'acesso-pi2')
        acessoJson = file2
        print('importação realizada do arquivo', file2['file'], 'enviado às', file2['updated'])
        
        with open('acesso.json', 'r') as openfile:
            json_object = json.load(openfile)
        
            print(json_object) 
            print(type(json_object)) 
        
        sql = """ INSERT INTO tb_user_catraca(usuario_cpf, catraca_idcatraca, datahora) VALUES (%s, %s, %s) RETURNING id_user_catraca; """
        conexao = None

        try:
            conexao = conecta_db()
            cursor = conexao.cursor()
            cursor.execute(sql, (json_object['cpf'], json_object['idCatraca'], json_object['dataHora'],))
            vendor_id = cursor.fetchone()[0]
            conexao.commit()
            cursor.close()
            print(vendor_id)

        except (Exception, psycopg2.DatabaseError) as error: 
            print(error)

        finally:
            if conexao is not None:
                conexao.close()
        

    else:
        acessoJson = file2
        # print('else', lastDat)

# dowload_file('acesso.json', '/home/others/Desktop/Raspberry/acesso.json', 'acesso-pi2')
acessoJson = null

schedule.every(0).minutes.do(lambda: compare_download_file(acessoJson, list_file('acesso-pi2')[0]))
# schedule.every(0).minutes.do(lambda: )

while True:
    schedule.run_pending()
    time.sleep(10)


# reconhecimento
# def upload_pickle(blob_name, file_path, bucket_name):
#     try:
#         bucket = storage_client.get_bucket(bucket_name)
#         blob = bucket.blob(blob_name)
#         blob.upload_from_filename(file_path)
#         print('true')
#         return True
#     except Exception as e:
#         print(e)
#         return False

# dictionary = { 
#     "idCatraca" : 1,
#     "cpf" : '123', 
#     "dataHora" : datetime.now()     
# } 
# json_object = json.dumps(dictionary, indent = 2, sort_keys=True, default=str)
# with open("acesso.json", "w") as outfile: 
#     outfile.write(json_object)

# upload_pickle('acesso.json', '/home/others/Desktop/Raspberry/acesso.json', 'acesso-pi2')