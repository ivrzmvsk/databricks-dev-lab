"""Check DEV OAuth without printing tokens or credential values."""

import json

import requests

from lab11.common import HOST, credentials, dev_client


def main():
    client_id, secret = credentials()
    w = dev_client()
    principals = list(w.service_principals.list(filter=f'applicationId eq "{client_id}"'))
    print(
        json.dumps(
            {
                "matching_service_principals": len(principals),
                "active": [p.active for p in principals],
                "entitlements": [[e.value for e in (p.entitlements or [])] for p in principals],
            }
        ),
        flush=True,
    )
    response = requests.post(
        HOST + "/oidc/v1/token",
        auth=(client_id, secret),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=30,
    )
    print(
        json.dumps(
            {
                "oauth_http_status": response.status_code,
                "access_token_received": response.ok and bool(response.json().get("access_token")),
            }
        ),
        flush=True,
    )
    if not response.ok:
        raise SystemExit("OAuth check failed; response body intentionally omitted")
    if len(principals) != 1 or not principals[0].active:
        raise SystemExit("Expected one active DEV service principal")


if __name__ == "__main__":
    main()
