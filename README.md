# Kafka Monitor
Kafka Monitor is a multi-threaded API that provides information about kafka instances, including topics defined and throughput of records.

## Usage
From the root of the directory run:
```bash
mvn clean package
```
And then execute the jar from the from targets directory.

Configuration is done through the endpoint "/config" and takes a document of the form:
```json
{
  "bootstrapServer": "localhost:9092",
  "startFromBeginning": "true",
  "ignoreInternalTopics": "true",
  "ignoreTopics": ["topic1", "topic2"]
}
```
The fields should be self explanatory.
