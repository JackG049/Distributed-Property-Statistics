mvn package -pl ../balancer/
docker build --tag balancer:latest .
