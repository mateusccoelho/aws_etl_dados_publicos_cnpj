#!/bin/bash

chmod u=rwx,go=r check_update.py
python3.9 -m venv env_check
source env_check/bin/activate
pip install -r requirements.txt
deactivate
cd env_check/lib/python3.9/site-packages
zip -r ../../../../check-deploy-pack.zip .
cd ../../../../
zip -g check-deploy-pack.zip check_update.py
mv check-deploy-pack.zip ../../artifacts/check-deploy-pack.zip