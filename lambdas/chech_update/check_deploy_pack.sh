#!/bin/bash

rm -f check-deploy-pack.zip
cd env_check/lib/python3.9/site-packages
zip -r ../../../../check-deploy-pack.zip .
cd ../../../../
zip -g check-deploy-pack.zip check_update.py
