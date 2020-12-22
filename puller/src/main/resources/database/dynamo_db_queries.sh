#!/bin/bash

# Create table
aws dynamodb create-table \
    --table-name daft_ie \
    --attribute-definitions AttributeName=County,AttributeType=S AttributeName=ListingDate,AttributeType=S \
    --key-schema AttributeName=County,KeyType=HASH AttributeName=ListingDate,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
		--endpoint-url http://localhost:8000

# List tables 
aws dynamodb list-tables --endpoint-url http://localhost:8000

# Put item 
#
#aws dynamodb batch-write-item \
#		--endpoint-url http://localhost:8000\
#    --request-items file://items.json \
#    --return-consumed-capacity TOTAL \
#    --return-item-collection-metrics SIZE
#
#
#aws dynamodb scan \
#		--endpoint-url http://localhost:8000 \
#    --table-name daft_ie 
#
#
#aws dynamodb scan \
#    --table-name daft_ie \
#    --filter-expression "ListingDate BETWEEN :startdate AND :enddate" \
#    --expression-attribute-values '{
#        ":startdate": { "S": "2020-11-24" },
#        ":enddate": { "S": "2020-11-26" }
#    }' 
