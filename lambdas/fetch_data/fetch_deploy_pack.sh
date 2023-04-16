#!/bin/bash

rm -f fetch-deploy-pack.zip
cd env_fetch/lib/python3.9/site-packages
zip -r ../../../../fetch-deploy-pack.zip .
cd ../../../../
zip -g fetch-deploy-pack.zip fetch_data.py
