#!/bin/bash

(cd ..; docker-compose -f dynamo-docker-compose.yml up &)
(cd ../puller; ./build.sh )
(cd ../puller; docker-compose -f puller_compose.yml up)

