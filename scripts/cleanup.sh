echo "Cleaning up docker stuff..."
docker-compose -f statistics/statistics_compose.yml down
docker-compose -f balancer/balancer-compose.yml down
docker-compose -f client/client_compose.yml down
docker-compose -f puller/puller_compose.yml down
docker-compose -f dynamo-docker-compose.yml down
echo "Shutting down kafka..."
docker-compose down
echo "Finished shutting down, have a nice day :)"