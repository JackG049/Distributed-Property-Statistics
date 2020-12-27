FROM openjdk:11.0.8-jre
COPY target/puller-1.0.jar /puller-1.0.jar
CMD java -jar /puller-1.0.jar --hostname=$KAFKA_HOST
