#!/bin/bash

chmod u=rwx,go=r fetch_data.py
python3.9 -m venv env_fetch
source env_fetch/bin/activate
pip install -r requirements.txt
deactivate
cd env_fetch/lib/python3.9/site-packages
zip -r ../../../../fetch-deploy-pack.zip .
cd ../../../../
zip -g fetch-deploy-pack.zip fetch_data.py
mv fetch-deploy-pack.zip ../../artifacts/fetch-deploy-pack.zip
