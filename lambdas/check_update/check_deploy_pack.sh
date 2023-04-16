#!/bin/bash

python3.9 -m venv env_check
source env_check/bin/activate
pip install -r requirements.txt
deactivate
cd env_check/lib/python3.9/site-packages
zip -r ../../../../../../artifacts/check-deploy-pack.zip .
cd ../../../../../../artifacts
zip -g check-deploy-pack.zip ../lambdas/check_update/check_update.py
