#!/bin/bash

pipenv run python heartbeat.py > heartbeat.log 2>&1 &
pipenv run python kvs.py
