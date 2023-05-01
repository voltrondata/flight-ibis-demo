#!/bin/bash

certbot certonly --manual \
                 --preferred-challenges=dns \
                 --work-dir=work \
                 --logs-dir=logs \
                 --config-dir=config
