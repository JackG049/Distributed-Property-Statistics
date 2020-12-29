#!/bin/bash

(cd ../puller; docker-compose -f dynamo_compose.yml up &)
(cd ../puller; ./build.sh )
(cd ../puller; docker-compose -f puller_compose.yml up)

