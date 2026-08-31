# 4. Spark and Confluent

Spark is the batch alternative for larger transformations and ML-oriented work:

```bash
bin/demo spark
```

Confluent is the event-streaming path. Kafka and Schema Registry govern events;
Flink/Tableflow-style processing materializes open tables that watsonx.data can
consume. It is separately operated and licensed from watsonx.data.

```bash
bin/demo streaming
bin/demo validate
```
