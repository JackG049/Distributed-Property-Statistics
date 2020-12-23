mvn package
cd statistics/ || exit
docker build --tag statistics:latest .
cd ..
echo "Starting up Distributed Property Statistics XD"
docker-compose up -d
echo "Allowing for Kafka to create topics..."
sleep 10s
echo "Risking it for a biscuit."
echo "Starting the statistics service..."
docker-compose -f statistics/statistics_compose.yml up

