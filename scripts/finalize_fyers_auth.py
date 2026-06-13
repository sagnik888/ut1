import sys

from authenticate_fyers import run


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/finalize_fyers_auth.py <auth_code_or_redirect_url>")
        sys.exit(1)
    run(sys.argv[1])
