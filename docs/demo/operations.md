# Operations and validation

Every demo stage has a local validation point:

```bash
bin/demo dbt debug
bin/demo validate
docker compose config -q
mkdocs build --strict
```

Use `bin/demo reset --dry-run` before cleanup. Component stacks can start alone
with their own Compose files, while the root `docker-compose.yml` includes them
for an all-in-one environment.
