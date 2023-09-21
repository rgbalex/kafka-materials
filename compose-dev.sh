#!/bin/bash

docker compose -f compose-dev.yaml -p kafka-materials "$@"
