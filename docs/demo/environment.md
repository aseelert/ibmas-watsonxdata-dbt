# Environment setup

The setup step prepares local dependencies, the watsonx.data certificate, and
the connection environment. It does not create schemas or start containers.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
bin/demo setup
```

Use the OpenShift preparation helper to refresh credentials before privileged
commands. Do not commit `.env`, tokens, API keys, generated certificates, or
connection exports.

Before a destructive reset, inspect the precise scope:

```bash
bin/demo reset --dry-run
```

See [Access and interfaces](access.md) for local services and ports.
