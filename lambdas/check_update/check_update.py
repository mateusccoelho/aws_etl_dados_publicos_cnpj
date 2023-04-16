import logging
import json

from bs4 import BeautifulSoup
from urllib.request import urlopen

if(logging.getLogger().hasHandlers()):
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

def lambda_handler(event, context):
    page = urlopen('https://dadosabertos.rfb.gov.br/CNPJ/')
    html = page.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    
    ref_date = None
    table_rows = soup.find_all('tr')
    for row in table_rows[3:]: # skipping header and navegation rows
        columns = row.find_all('td')
        file_name = columns[1].text.lower()
        if('empresas0' in file_name):
            ref_date = int(columns[2].text[:10].replace('-', ''))
            break
        
    return {
        'statusCode': 200,
        'body': {'ref_date': ref_date}
    }


if(__name__ == '__main__'):
    print(lambda_handler({}, {}))