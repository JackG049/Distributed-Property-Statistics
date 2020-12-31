# Distributed Property Statistics

Computes Irish property statistics in a distributed manner. A video 
demonstration of the system is included in this directory as dps-video-demo.mp4. A report detailing the most important aspects of this project is included in this directory as Distributed\_Property\_Statistics.pdf.


## Installation
**Prerequisites**
- Java 11
- Maven
- Docker 
- docker-compose
- patience :)

To build and run the entire application, simply execute the command from the root directory
i.e from the distributed-property-statistics directory
```
./scripts/do_the_thing.sh
```

## Usage

Once the application has successfully launched, visit [http://localhost:8080]()
to run some property queries. Remember to fill in each field. Dates must conform to 
the format YYYY-MM-DD e.g 2020-03-21; Prices represent property prices e.g 450000.


## Required Ports
```
Client = 8080
Load Balancer = 8081
Broker (Puller) = 8082+
Kafka = 9000-9093
```
