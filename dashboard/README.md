# Whoop Analytics dashboard

Next.js app (App Router, Cache Components) that reads the daily Gold/Silver export
from a private Cloudflare R2 bucket and renders the four sections defined in the
repo root's `CLAUDE.md` ("Dashboard content design"): Pipeline Health, Today,
Trends, Derived Insights.

## Local development

```bash
npm install
npm run dev
```

Requires a `.env.local` (see `.env.example`) with read-only R2 credentials.

## Data flow

- `lib/r2.ts` fetches each exported JSON file from R2 via the S3-compatible SDK,
  wrapped in a `"use cache"` function tagged `dashboard-data`.
- `app/api/revalidate/route.ts` is called by the Databricks export job
  (`notebooks/02_export_dashboard_data.py`) after each daily run, invalidating
  that cache tag so new data is live within seconds — no redeploy involved.

## Deploy

Deployed on Vercel via its GitHub integration (Root Directory: `dashboard`).
Required environment variables: `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`,
`R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`, `REVALIDATE_SECRET`.
