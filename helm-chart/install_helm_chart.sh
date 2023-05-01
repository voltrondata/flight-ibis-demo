#!/bin/bash

kubectl config set-context --current --namespace=flight

helm upgrade test --install .
