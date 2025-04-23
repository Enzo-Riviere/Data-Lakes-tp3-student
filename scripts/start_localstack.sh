#!/bin/bash

# Check if a LocalStack container is already running
if [ "$(docker ps -q -f name=jolly_ritchie)" ]; then
    echo "LocalStack is already running."
else
    # Check if the container exists but is stopped
    if [ "$(docker ps -aq -f name=jolly_ritchie)" ]; then
        echo "LocalStack container exists but is stopped. Restarting..."
        docker start jolly_ritchie
        aws --endpoint-url=http://localhost:4566 s3 mb s3://raw
        aws --endpoint-url=http://localhost:4566 s3 mb s3://staging
        aws --endpoint-url=http://localhost:4566 s3 mb s3://curated
    else
        echo "LocalStack container does not exist. Starting a new container..."
        docker run -d -p 4566:4566 -p 4572:4572 --name jolly_ritchie localstack/localstack
        aws --endpoint-url=http://localhost:4566 s3 mb s3://raw
        aws --endpoint-url=http://localhost:4566 s3 mb s3://staging
        aws --endpoint-url=http://localhost:4566 s3 mb s3://curated
    fi
fi
