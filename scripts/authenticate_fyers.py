import sys
import hashlib
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def run(auth_code: str):
    from config.settings import get_settings

    settings = get_settings()
    app_id = settings.fyers_app_id
    secret_key = settings.fyers_secret_key

    code = (auth_code or "").strip()
    if "auth_code=" in code or "code=" in code:
        query = parse_qs(urlparse(code).query)
        code = query.get("auth_code", query.get("code", [code]))[0]

    app_id_hash = hashlib.sha256(f"{app_id}:{secret_key}".encode()).hexdigest()
    payload = {
        "grant_type": "authorization_code",
        "appIdHash": app_id_hash,
        "code": code,
    }

    url = "https://api-t1.fyers.in/api/v3/validate-authcode"
    print(f"Validating Auth Code for {app_id}...")
    response = requests.post(url, json=payload, timeout=30).json()

    if response.get("s") == "ok":
        token_data = {
            "access_token": response.get("access_token"),
            "refresh_token": response.get("refresh_token"),
            "timestamp": datetime.now().isoformat(),
        }
        with open("fyers_token.json", "w") as f:
            json.dump(token_data, f, indent=2)
        print("SUCCESS: fyers_token.json updated!")
        return True

    print(f"FAILED: {response}")
    return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/authenticate_fyers.py <auth_code>")
        sys.exit(1)
    run(sys.argv[1])
