#!/usr/bin/env python3
"""CLI utility to generate RS256 JWT tokens for testing the API.

Usage:
    uv run python scripts/generate_token.py --role analytics_engineer
    uv run python scripts/generate_token.py --role data_engineer --hours 72
"""

from __future__ import annotations

import argparse
import time

import jwt as pyjwt

PRIVATE_KEY_PEM = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCp17PsSTf3e03m
wR76GCgm3zpASYab1XkGJirst/NZvQZ88A1u2QTiQeWhO7TDLXinko2n0ZFxNZSX
2/wQcBMKCnwWxq/xFE6b73zHQkoduj+YQj2f+8xvY+Iq0oEyIi6DKKFm27jsd+uY
CYauZnr9dKKbv7ruv+L0KgwosCxqrCsxNhDZl/08/lSb2LXfIybJuh6VMQBRLqkT
15pDIybwSGCjy4BgIyUEqwjOc+AcoYDMv0107TWMu4IaCvgiUPZihzZZsqAV090l
yiuyF53+rv84oLL+zHy/NG7Mpii7vJnTaUPf9bBFW7MLwjwdlkh4ov4/MSJqsITy
Y+oJG3adAgMBAAECggEABDMZt1N+J0fsvrJyxiNXxtJJOfK3ed327qB9+jl4MnVa
ljdHVcDW/pM7jtePmi3jKF2W1Bn5+y8ke/bMDkn/JoXo2JVUH2VtpixvTOwGMiL7
VJP6uxx6SxzQqFdpK2it9r9H8mendG1orWs64dAV5XN/W9OLV0D2Zyws/cqRZpfN
5aZyf1871UvHQgK49kjWQ69ipGZM92bc/vESGxpAZeKKYSYXtkkWxMzpAR7SeSZ5
zIQrd5cX94OzKhoGqAGQUTWTetfBTIsczRu0K+bDBwwE59nMtUQ3M5F5ic3fEQMR
WdF6cowUPB8yHFHsEVY3boA9VATO3EQxnDLENCzCrwKBgQDjj2/7e32EaH7HUkUv
p3hEeztKgf/1N7JvIlo5Sa11v50QKhwAicKYgaLfTmddtzXdrnt8cZQ+OGnR+qGn
90IaY1zcnYEHk6UTldN6h3v0aFQTUzMG2OcAgJsV66hzxg1DyMpnG1Fa5XAmRZll
1rbOMJz2Ck9B5LU3ZkRvygXjDwKBgQC/EaUzfZVED7i7DgW+xY/IjZVJzQ8tvkfz
1TOYtmvlxkg4v8CVLvQ/b+N2qqaZn3wTH9mAU0YUOM4Q1dfvPrD4d+A63Rg32+1U
tEwc46/5PMaCtGxmO7WLccFgk1wyaTkc30h8jofuqJmaR0y3HVv/0M29meLsR+N3
0q3AFMCbkwKBgQDDGvJKTiDZ67X3M4R6TT4CiR3WzgsktjJYsr1krNT6ReVmPJRx
qaucklmQ2Goroa+fd8AMfF0706Z3EEqV9ptIgLTXunssgdxhJG6DebI/ZUvgnc78
KfA1MA7IBpsRWFd7LKbNLFDefCVhyv6woB1wP6H0GfbGak8tRpOavT265QKBgGj1
Z3umk/WEcWUH6e4HFtoDtKuK4ritG1d9mc9c/l6Fkqzh4QfSeEfUze4lBknDi2Py
DgfpNsjq/3/OCMWa+Zo0N8/+HkypGnF6bYk9JjDSyvWH6Tgruqm0Ppcvu+jRVpde
rLIHlfJrWZ2fZyv8C8q2SB7MRxSm1PTAncOzYq7TAoGBAODoOW0Knt4TdFh3cdbF
GFWEULjJG5Y5AasIKRn8QpjCOaKVwib78gJZtj9DalUFiJ6pYsTd4YibB5/2XVLm
UHROCgh5z7TbPnCEobz5nLv0Z3ZGuAZJiUD4mNNAKhtLE0BXpzSQBy9wl2a56HCZ
nqPPnQGKt6gwFDkPJwzkr4lY
-----END PRIVATE KEY-----"""


def generate_token(role: str = "analytics_engineer", hours: int = 24) -> str:
    payload = {
        "sub": f"user_{role}",
        "email": f"{role}@company.com",
        "roles": [role],
        "exp": int(time.time()) + (hours * 3600),
    }
    return pyjwt.encode(payload, PRIVATE_KEY_PEM, algorithm="RS256")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate JWT Bearer token for API authentication."
    )
    parser.add_argument(
        "--role", default="analytics_engineer", help="User role (default: analytics_engineer)"
    )
    parser.add_argument(
        "--hours", type=int, default=24, help="Token validity in hours (default: 24)"
    )
    args = parser.parse_args()

    token = generate_token(role=args.role, hours=args.hours)
    print(f"\n--- JWT Token for role '{args.role}' (valid for {args.hours}h) ---")
    print(token)
    print("\n--- Usage in HTTP Header ---")
    print(f"Authorization: Bearer {token}\n")


if __name__ == "__main__":
    main()
