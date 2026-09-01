# Access: URLs and ports

Use this page as the demo console index. Services are available only after the
corresponding command has been started. All URLs are local unless noted.

All Docker services share one project (`ibmas-watsonxdata-dbt`) and are managed
with `bin/demo docker start|stop|status`. Start individual services or all at once:

```bash
bin/demo docker start              # all services
bin/demo docker start metabase     # one service
bin/demo docker status             # check what is running
bin/demo docker stop               # stop all
```

| Audience | Service | URL / port | Start command | Purpose |
| --- | --- | --- | --- | --- |
| Business user | Metabase | [http://localhost:3000](http://localhost:3000) | `bin/demo docker start metabase` | Gold dashboard and ad-hoc analysis |
| Demo operator | Airflow | [http://localhost:8082](http://localhost:8082) | `bin/demo docker start airflow` | DAG schedule, runs, and task logs |
| Demo operator | Kafbat | [http://localhost:28080](http://localhost:28080) | `bin/demo docker start streaming` | Kafka topics, consumers, and messages |
| Demo operator | Flink | [http://localhost:28085](http://localhost:28085) | `bin/demo docker start streaming` | Streaming jobs, checkpoints, and task state |
| Data engineer | Marquez | [http://localhost:3001](http://localhost:3001) | `bin/demo docker start lineage` | Runtime lineage graph |
| Data steward | OpenMetadata | [http://localhost:8585](http://localhost:8585) | `bin/demo docker start catalog` | Catalog, dbt lineage, glossary, and governance |
| Platform user | watsonx.data | CPD URL configured for the environment | `bin/demo setup` | Iceberg catalog, Presto, Spark applications, and object storage |

## Service APIs and technical endpoints

These are operator or integration endpoints, not the primary demo UI.

| Service | Endpoint | Use |
| --- | --- | --- |
| Kafka | `localhost:29092` | Producer and consumer bootstrap server |
| Schema Registry | [http://localhost:28081](http://localhost:28081) | Avro schema API |
| Flink SQL Gateway | [http://localhost:28083](http://localhost:28083) | SQL Gateway API |
| Iceberg REST catalog | [http://localhost:28181](http://localhost:28181) | Local Flink Iceberg catalog API |
| Marquez API | [http://localhost:5010](http://localhost:5010) | OpenLineage event and lineage API |
| Marquez admin API | [http://localhost:5012](http://localhost:5012) | Administrative API |
| OpenMetadata API | `http://localhost:8585/api` | Catalog API used by ingestion |
| OpenMetadata ingestion | `http://localhost:8080` | Internal ingestion service; not a business UI |

The OpenMetadata MySQL (`3306`) and Elasticsearch (`9200`, `9300`) ports are
published for local troubleshooting only. They are not demo entry points.

## Recommended presentation order

1. watsonx.data / dbt execution
2. Metabase dashboard
3. Spark or Kafbat and Flink, depending on the processing story
4. Airflow orchestration
5. Marquez runtime lineage
6. OpenMetadata catalog and governance
