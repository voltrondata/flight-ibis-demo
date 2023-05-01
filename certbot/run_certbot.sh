#!/bin/bash

certbot certonly --manual \
                 --preferred-challenges=dns \
                 --email=philip@voltrondata.com \
                 --key-type=rsa \
                 --work-dir=work \
                 --logs-dir=logs \
                 --config-dir=config
