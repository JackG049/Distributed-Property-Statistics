FROM openjdk:11.0.8-jre
ENV CONSUMER_TOPIC "requests_daft"
COPY target/statistics-1.0-jar-with-dependencies.jar /statistics-1.0.jar
CMD java -cp /statistics-1.0.jar com.dsp.service.Statistics  ${CONSUMER_TOPIC}