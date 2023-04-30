import re
import logging
import pprint

from bs4 import BeautifulSoup
from urllib.request import urlopen

if(logging.getLogger().hasHandlers()):
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

ALLOWED_TABLE_NAMES = [
    'cnaes', 'empresas', 'estabelecimentos', 'motivos', 
    'municipios', 'naturezas', 'paises', 'qualificacoes', 
    'simples', 'socios'
]
CNPJ_DATA_REPOSITORY = 'https://dadosabertos.rfb.gov.br/CNPJ/'

def lambda_handler(event, context):
    page = urlopen(CNPJ_DATA_REPOSITORY)
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")

    tables_to_parse = event['Tables']
    tables_in_glue = {metadata_dict['Name'] for metadata_dict in event['DBOutput']['TableList']}
    result_dict = {}
    for table in tables_to_parse:
        if(table in ALLOWED_TABLE_NAMES):
            result_dict[table] = {
                'name': table,
                'exists': table in tables_in_glue,
                'files': [],
                'ref_date': 0
            }

    # first three rows are header, navegation and footer rows
    # column 0 is file icon, 1 is file name and 2 is last modified date
    table_rows = soup.find_all('tr')[3:-1]
    for row in table_rows: 
        columns = row.find_all('td')

        # skipping folders
        if('folder' in columns[0].img['src']):
            continue

        raw_file_name = columns[1].a.text
        parsed_file_name = re.sub(
            pattern = '[^a-zA-Z]', 
            repl = '', 
            string = raw_file_name.split('.')[0].lower() 
        )
        if(parsed_file_name not in result_dict):
            continue
        
        link = CNPJ_DATA_REPOSITORY + columns[1].a['href']
        ref_date = int(columns[2].text[:10].replace('-', ''))

        table_dict = result_dict[parsed_file_name]
        table_dict['files'].append(link)
        if(ref_date > table_dict['ref_date']):
            table_dict['ref_date'] = ref_date

    for name, table_dict in list(result_dict.items()):
        if(not table_dict['files'] or table_dict['ref_date'] == 0):
            del result_dict[name]
        
    return {
        'statusCode': 200,
        'body': {'Tables': list(result_dict.values())}
    }


if(__name__ == '__main__'):
    pprint.pp(lambda_handler(
        {
            "Tables": [
                "empresas",
                "cnaes",
                "municipios"
            ],
            "DBOutput": {
                "TableList": [
                {
                    "CatalogId": "598433695633",
                    "CreateTime": "2023-04-23T22:20:15Z",
                    "CreatedBy": "arn:aws:sts::598433695633:assumed-role/CNPJCrawlerRole/AWS-Crawler",
                    "DatabaseName": "cnpj",
                    "IsRegisteredWithLakeFormation": False,
                    "LastAccessTime": "2023-04-23T22:20:15Z",
                    "Name": "empresas",
                    "Owner": "owner",
                    "Parameters": {
                    "sizeKey": "1608647538",
                    "objectCount": "10",
                    "UPDATED_BY_CRAWLER": "EmpresasCrawler",
                    "CrawlerSchemaSerializerVersion": "1.0",
                    "recordCount": "53293844",
                    "averageRecordSize": "47",
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "compressionType": "none",
                    "classification": "parquet",
                    "typeOfData": "file"
                    },
                    "PartitionKeys": [
                    {
                        "Name": "ref_date",
                        "Type": "string"
                    }
                    ],
                    "Retention": 0,
                    "StorageDescriptor": {
                    "BucketColumns": [],
                    "Columns": [
                        {
                        "Name": "cnpj_raiz",
                        "Type": "bigint"
                        },
                        {
                        "Name": "raz_soc",
                        "Type": "string"
                        },
                        {
                        "Name": "nat_jud",
                        "Type": "bigint"
                        },
                        {
                        "Name": "qualif_resp",
                        "Type": "bigint"
                        },
                        {
                        "Name": "cap_soc",
                        "Type": "double"
                        },
                        {
                        "Name": "porte",
                        "Type": "bigint"
                        },
                        {
                        "Name": "ent_fed",
                        "Type": "string"
                        }
                    ],
                    "Compressed": False,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "Location": "s3://projeto-cnpj/cnpj_db/empresas/",
                    "NumberOfBuckets": -1,
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "Parameters": {
                        "sizeKey": "1608647538",
                        "objectCount": "10",
                        "UPDATED_BY_CRAWLER": "EmpresasCrawler",
                        "CrawlerSchemaSerializerVersion": "1.0",
                        "recordCount": "53293844",
                        "averageRecordSize": "47",
                        "CrawlerSchemaDeserializerVersion": "1.0",
                        "compressionType": "none",
                        "classification": "parquet",
                        "typeOfData": "file"
                    },
                    "SerdeInfo": {
                        "Parameters": {
                        "serialization.format": "1"
                        },
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                    "SortColumns": [],
                    "StoredAsSubDirectories": False
                    },
                    "TableType": "EXTERNAL_TABLE",
                    "UpdateTime": "2023-04-23T22:20:15Z",
                    "VersionId": "0"
                }
                ]
            }
        }, 
        {}
    ))