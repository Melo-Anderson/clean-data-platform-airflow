#!/bin/sh
# scripts/init_openbao.sh — dev-only bootstrap for OpenBao (Vault compatible)
# In production: use dynamic secrets and Vault policies instead.
set -e

python3 - << 'PYEOF'
import json
import os
import sys
import time
import urllib.error
import urllib.request

vault_addr = os.environ.get("PLATFORM_VAULT_URL", "http://openbao:8200").rstrip("/")
vault_token = os.environ.get("PLATFORM_VAULT_TOKEN", "root")

print(f"Waiting for OpenBao at {vault_addr}...")
health_url = f"{vault_addr}/v1/sys/health"

ready = False
for i in range(30):
    try:
        req = urllib.request.Request(health_url)
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            if data.get("initialized") is True:
                print("OpenBao is ready.")
                ready = True
                break
    except Exception:
        time.sleep(1)

if not ready:
    print("Timed out waiting for OpenBao to become ready.", file=sys.stderr)
    sys.exit(1)

seed_file = "/seeds/openbao_secrets.json"
if not os.path.exists(seed_file):
    # Fallback path if running outside Docker container
    seed_file = os.path.join(os.path.dirname(__file__), "seeds", "openbao_secrets.json")

print(f"Seeding secrets from {seed_file}...")
with open(seed_file, encoding="utf-8") as f:
    secrets = json.load(f)

for entry in secrets:
    path = entry["path"].lstrip("/")
    payload = json.dumps({"data": entry["data"]}).encode()
    req = urllib.request.Request(
        f"{vault_addr}/v1/{path}",
        data=payload,
        headers={"X-Vault-Token": vault_token, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"  OK [{resp.status}] -> {path}")
    except urllib.error.HTTPError as e:
        print(f"  FAIL [{e.code}] -> {path}: {e.read().decode()}", file=sys.stderr)
        sys.exit(1)

print("OpenBao seeding complete.")
PYEOF
