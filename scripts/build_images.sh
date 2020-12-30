echo "Packaging"
mvn package
echo "Building balancer image..."
docker build --tag balancer:latest balancer/ || exit
echo "Building client image..."
docker build --tag client:latest client/ || exit
echo "Building puller image..."
docker build --tag puller:latest puller/ || exit
echo "Building statistics image..."
docker build --tag statistics:latest statistics/ || exit

