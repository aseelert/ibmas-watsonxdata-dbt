# 1. Prepare

Copy `.env.example` to `.env`, provide watsonx.data connection details, and
save the exported connection JSON under `watsonx_data/`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
bin/demo setup
```

`setup` prepares the certificate and environment. It does not create remote
schemas or start containers. Review destructive actions separately with
`bin/demo reset --dry-run`.
