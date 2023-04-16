#!/bin/bash

python3.9 -m venv env_fetch
source env_fetch/bin/activate
pip install -r requirements.txt
deactivate
cd env_fetch/lib/python3.9/site-packages
zip -r ../../../../../../artifacts/fetch-deploy-pack.zip .
cd ../../../../../../artifacts
zip -g fetch-deploy-pack.zip ../lambdas/fetch_data/fetch_data.py
