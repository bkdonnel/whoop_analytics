"""One-time interactive OAuth2 authorization-code exchange for the Whoop API.

Run manually, once: opens the browser for the user to approve access, catches
the redirect on localhost, exchanges the code for tokens, and stores the
resulting refresh token in the Databricks secret scope via the Databricks CLI.
Everything recurring (token refresh, ingestion) happens in Databricks notebooks
instead, since this step needs a browser redirect back to localhost.
"""

from __future__ import annotations

import asyncio
import http.server
import os
import subprocess
import urllib.parse
import webbrowser

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel


async def main() -> None:
    load_dotenv()
    config = load_oauth_config()

    state = os.urandom(16).hex()
    authorization_url = build_authorization_url(config, state)
    print(f"Opening browser for Whoop authorization:\n{authorization_url}\n")
    webbrowser.open(authorization_url)

    port = urllib.parse.urlparse(config.redirect_uri).port or 8080
    print(f"Waiting for redirect on localhost:{port}...")
    callback_result = run_callback_server(port)

    has_code = callback_result.code is not None
    if not has_code:
        raise SystemExit(f"Authorization failed: {callback_result.error}")

    print("Authorization code received, exchanging for tokens...")
    tokens = await exchange_code_for_tokens(callback_result.code, config)

    print(f"Access token expires in {tokens.expires_in}s. Granted scope: {tokens.scope}")
    store_refresh_token(tokens.refresh_token)
    print("Done. whoop_refresh_token stored in the bryan_whoop_analytics secret scope.")


def load_oauth_config() -> "WhoopOAuthConfig":
    return WhoopOAuthConfig(
        client_id=os.environ["WHOOP_CLIENT_ID"],
        client_secret=os.environ["WHOOP_CLIENT_SECRET"],
        redirect_uri=os.environ["WHOOP_REDIRECT_URI"],
    )


def build_authorization_url(config: "WhoopOAuthConfig", state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "scope": OAUTH_SCOPES,
        "state": state,
    }
    return f"{AUTHORIZATION_URL}?{urllib.parse.urlencode(params)}"


def run_callback_server(port: int) -> "CallbackResult":
    received: dict[str, str | None] = {"code": None, "error": None}

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            received["code"] = query.get("code", [None])[0]
            received["error"] = query.get("error", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            has_code = received["code"] is not None
            message = "Authorization received, you can close this tab." if has_code else f"Authorization failed: {received['error']}"
            self.wfile.write(message.encode())

        def log_message(self, log_format: str, *args: object) -> None:
            return

    server = http.server.HTTPServer(("localhost", port), CallbackHandler)
    server.handle_request()
    server.server_close()
    return CallbackResult(code=received["code"], error=received["error"])


async def exchange_code_for_tokens(code: str, config: "WhoopOAuthConfig") -> "TokenResponse":
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": config.client_id,
                "client_secret": config.client_secret,
                "redirect_uri": config.redirect_uri,
            },
        )
    response.raise_for_status()
    return TokenResponse.model_validate(response.json())


def store_refresh_token(refresh_token: str) -> None:
    command = [
        "/opt/homebrew/bin/databricks",
        "secrets",
        "put-secret",
        SECRET_SCOPE,
        "whoop_refresh_token",
        "--string-value",
        refresh_token,
        "--profile",
        DATABRICKS_PROFILE,
    ]
    print(f"Storing refresh token: databricks secrets put-secret {SECRET_SCOPE} whoop_refresh_token --string-value <redacted> --profile {DATABRICKS_PROFILE}")
    subprocess.run(command, check=True)


AUTHORIZATION_URL = "https://api.prod.whoop.com/oauth/oauth2/auth"
TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
OAUTH_SCOPES = "read:recovery read:cycles read:sleep read:workout read:profile read:body_measurement offline"
DATABRICKS_PROFILE = "dbc-7b106152-caf3"
SECRET_SCOPE = "bryan_whoop_analytics"


class WhoopOAuthConfig(BaseModel):
    client_id: str
    client_secret: str
    redirect_uri: str


class CallbackResult(BaseModel):
    code: str | None
    error: str | None


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    expires_in: int
    scope: str
    token_type: str


if __name__ == "__main__":
    asyncio.run(main())
