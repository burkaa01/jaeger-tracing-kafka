# kafka-connect

This is an example of configuration for Kafka Connect. The configuration for the file source connector is already setup in `./file-source/file-source.json`. The following curl command will configure the connector pointed at the `numbers.txt` file (which is mounted to the docker container at `/usr/share/data/file-source`):

```bash
cd ./file-source
curl -X POST -H "Content-Type: application/json" --data @file-source.json http://localhost:8083/connectors
curl http://localhost:8083/connectors
```

The configuration for the Elasticsearch sink connector is configured in `./elastic-sink/elastic-sink.json`. The following curl command will configure the connector:

```bash
cd ./elastic-sink
curl -X POST -H "Content-Type: application/json" --data @elastic-sink.json http://localhost:8083/connectors
curl http://localhost:8083/connectors
```
