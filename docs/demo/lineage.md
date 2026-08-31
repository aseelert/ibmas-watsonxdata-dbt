# 6. Runtime lineage with OpenLineage and Marquez

Marquez visualizes runtime events. It complements dbt's compiled dependency
graph and does not replace a business catalog.

```bash
bin/demo lineage
```

Enable `OPENLINEAGE_URL` only when the Marquez collector is running. The dbt
launcher then emits events after successful artifact-producing commands.
