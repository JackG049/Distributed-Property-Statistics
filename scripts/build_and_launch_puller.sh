#!/bin/bash

(cd ../puller; ./build.sh )
(cd ../puller; docker-compose -f puller_compose.yml up)

