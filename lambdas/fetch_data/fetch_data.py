import os
import urllib.request
import zipfile
import logging

import boto3
import pyarrow.csv as pv
import pyarrow.parquet as pq

if(logging.getLogger().hasHandlers()):
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

COLUMNS = {
    'empresas': ['cnpj_raiz', 'raz_soc', 'nat_jud', 'qualif_resp', 'cap_soc', 'porte', 'ent_fed']
}

def lambda_handler(event, context):
    """ Receives the table name, url of the part that will be download and
        the timestamp that will be the partition key.
    """

    url = event['url']
    table_name = event['table_name']

    zip_filename = os.path.basename(url)
    zip_file_path = f'/tmp/{zip_filename}'
    
    logging.info(f'Starting download of {zip_filename}')
    urllib.request.urlretrieve(url, zip_file_path)
    
    logging.info('Unziping file')
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        csv_path = zip_ref.extract(csv_filename, '/tmp/')

    logging.info('Converting CSV to Parquet')
    table = pv.read_csv(
        csv_path,
        read_options = pv.ReadOptions(column_names=COLUMNS[table_name], encoding='latin-1'),
        parse_options = pv.ParseOptions(delimiter=';'),
        convert_options = pv.ConvertOptions(decimal_point=',')
    )
    orig_file_name = zip_filename.split('.')[0]
    parquet_file_path = f'/tmp/{orig_file_name}.parquet'
    pq.write_table(table, parquet_file_path)

    s3 = boto3.client('s3')
    partition_value = event['date']
    file_key = f'cnpj_db/{table_name}/ref_date={partition_value}/{orig_file_name}.parquet'
    s3.upload_file(parquet_file_path, 'cnpj-project', file_key)

if(__name__ == '__main__'):
    lambda_handler({
        'url':'https://dadosabertos.rfb.gov.br/CNPJ/Empresas4.zip', 
        'table_name':'empresas',
        'date': '20230409'
        }, {}
    )