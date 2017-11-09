# OTX-REPUTATION

OTX-REPUTATION is a service that pulls IP reputation data from AlienVault and send it to a Kafka topic. This information is sended as json events and can be readed by other services in order to use it.

##Compiling sources

To build this project you can use `maven` tool. 

If you want to build the JAR of the project you can do:

```
mvn clean package
```

If you want to check if it passes all the test:

```
mvn test
```

If you want to build the distribution tar.gz:

```
mvn clean package -P dist
```

If you want to build the docker image, make sure that you have the docker service running:

```
mvn clean package -P docker
```

##Usage
The service have two scripts at `bin` directory: `otx-generate-list.sh` and `otx-service-start.sh`. 

### otx-generate-list.sh
```
./bin/otx-generate-list.sh <out_file>
```
This script pulls the reputation IP data from AlientVault and writes it to the specified output file as the next example:

```
{"enrich_with":{"darklist_score":16,"darklist_score_name":"very low","darklist_category":"Malicious Host"},"ip":"107.198.76.6"},{"enrich_with":{"darklist_score":16,"darklist_score_name":"very low","darklist_category":"Malicious Host"}
```

### otx-service.start.sh
```
./bin/otx-service-start.sh config.json
```
This script starts the otx service reading the config from `config.json`. It sends the ip reputation info to the specified kafka topic at config as the next example:

```
91.74.55.232,{"otx_score_name":"very low","otx_category":"Malicious Host","otx_score":16}
116.72.4.32,{"otx_score_name":"very low","otx_category":"Malicious Host","otx_score":24}
```
The IP is the kafka key and the json is the value.

##Configuration
The configuration is defined as follows:
``` 
{
  "bootstrap.kafka.topics":["__reputation_otx_bootstrap"],
  "bootstrap.servers": "192.168.1.106:9092",
  "reputation.topic": "otx-reputation",
  "interval.ms": 1800000,
  "metric.enable": true,
  "metric.listeners": ["io.wizzie.reputation.otx.metrics.ConsoleMetricListener"],
  "metric.interval": 60000,
  "application.id": "reputation-service"
}
``` 
The most relevant fields are:

* bootstrap.kafka.topics: the topics used by otx reputation service in order to maintain its internal state
* bootstrap.servers: the kafka servers to connect to.
* reputation.topic: the topic that will be used to write the events.

## Contributing

1. [Fork it](https://github.com/wizzie-io/otx-reputation/fork)
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Create a new Pull Request


