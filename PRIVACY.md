# Privacy Policy

This is a personal analytics project. It is not a commercial product and is not distributed to other users.

## What this app does

This application connects to the Whoop API (via OAuth2) solely to read the account owner's own Whoop data — recovery, cycles, sleep, workout, profile, and body measurement data — for the purpose of building personal analytics and a personal dashboard.

## Data access and storage

- Data is pulled only for the Whoop account that explicitly authorizes this application via Whoop's OAuth consent flow.
- Ingested data is stored in the owner's own private Databricks workspace (Unity Catalog), not in any third-party or publicly accessible database.
- OAuth credentials (client ID, client secret, refresh token) are stored in a Databricks secret scope and are never committed to this repository.

## Data sharing

Data is not sold, shared, or disclosed to any third party. Any public-facing dashboard built from this data presents only aggregated or derived personal fitness metrics (e.g. recovery trends, strain load), not raw account credentials or personally identifying account details beyond what the owner chooses to display.

## Revoking access

The account owner can revoke this application's access at any time via their Whoop account's connected-apps settings, which immediately stops all further data access.

## Contact

Questions about this project's data handling can be raised via the repository's GitHub Issues page.
