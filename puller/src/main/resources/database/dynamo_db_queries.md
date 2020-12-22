# List tables 
aws dynamodb list-tables --endpoint-url http://localhost:8000

# Create table
aws dynamodb create-table \
    --table-name daft_ie \
    --attribute-definitions AttributeName=County,AttributeType=S AttributeName=ListingDate,AttributeType=S \
    --key-schema AttributeName=County,KeyType=HASH AttributeName=ListingDate,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
		--endpoint-url http://localhost:8000

# Put item 
aws dynamodb put-item \
		--endpoint-url http://localhost:8000\
    --table-name daft_ie \
    --item file://item.json \
    --return-consumed-capacity TOTAL \
    --return-item-collection-metrics SIZE


aws dynamodb batch-write-item \
		--endpoint-url http://localhost:8000\
    --request-items file://items.json \
    --return-consumed-capacity TOTAL \
    --return-item-collection-metrics SIZE


aws dynamodb scan \
		--endpoint-url http://localhost:8000 \
    --table-name daft_ie 



# Get item 

aws dynamodb get-item \--endpoint-url http://localhost:8000 \
--table-name daft_ie \
--key file://key.json \
--return-consumed-capacity TOTAL


# Query

aws dynamodb query \
		--endpoint-url http://localhost:8000 \
    --table-name daft_ie \
    --key-condition-expression "County = :v1" \
    --expression-attribute-values file://query.json \
    --return-consumed-capacity TOTAL

aws dynamodb query \
		--endpoint-url http://localhost:8000 \
    --table-name daft_ie \
    --key-condition-expression "County = :county AND ListingDate BETWEEN :startdate AND :enddate" \
    --expression-attribute-values '{
        ":county": { "S": "Galway" },
        ":startdate": { "S": "2020-11-24" },
        ":enddate": { "S": "2020-11-27" }
    }' 

aws dynamodb query \
		--endpoint-url http://localhost:8000 \
    --table-name daft_ie \
    --key-condition-expression "County = :county AND ListingDate BETWEEN :startdate AND :enddate" \
    --expression-attribute-values '{
        ":county": { "S": "Galway" },
        ":startdate": { "S": "2020-11-24" },
        ":enddate": { "S": "2020-11-27" }
    }' 



# Scan Query
aws dynamodb scan \
    --table-name daft_ie \
    --filter-expression "ListingDate BETWEEN :startdate AND :enddate" \
    --expression-attribute-values '{
        ":startdate": { "S": "2020-11-24" },
        ":enddate": { "S": "2020-11-26" }
    }' 
