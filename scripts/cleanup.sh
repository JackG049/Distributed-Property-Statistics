echo "Cleaning up docker stuff..."
docker-compose -f statistics/statistics_compose.yml down
echo "Shutdown statistics service"
echo "Shutting down kafka..."
docker-compose down
echo "Finished shutting down, have a nice day :)"