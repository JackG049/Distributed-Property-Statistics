echo "Doing the thing..."
sh scripts/build_images.sh || exit
echo "Finished building docker images"
echo "Launching services"
echo "Starting Kafka..."
docker-compose up -d
echo "Allowing time for topic creating"
sleep 10s
echo "Finished starting Kafka"
echo "Continuing with other services"
echo "Starting the Load Balancer"
docker-compose -f balancer/balancer-compose.yml up &

echo "Starting DynamoDB"
docker-compose -f dynamo-docker-compose.yml up &
sleep 4s
echo "Finished starting DynamoDB"
echo "Starting the Puller Service"
docker-compose -f puller/puller_compose.yml up &
echo "Starting Statistics service"
docker-compose -f statistics/statistics_compose.yml up &
echo "Starting Client service"
docker-compose -f client/client_compose.yml up &
sleep 3s
echo "Please open localhost:8080 in your preferred browser"

