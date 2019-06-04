#!/bin/bash

pipenv run python ./src/heartbeat.py > heartbeat.log 2>&1 &
pipenv run python ./src/app.py
