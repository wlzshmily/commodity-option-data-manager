# Project Context

## Purpose

Option Data Manager is a local data support service for a future option ratio spread monitoring platform. It collects, stores, exposes, and displays current China commodity option data slices.

## Approved Scope

- First release market scope: DCE, CZCE, SHFE, INE, and GFEX commodity options.
- Data source: TQSDK.
- Storage: local SQLite current slice, not a historical warehouse.
- UI: local WebUI using the current `src/option_data_manager/webui` implementation as the approved baseline.
- API: local FastAPI service, bound to `127.0.0.1` by default, with optional API Key enforcement.

## Explicit Non-Goals

- Trading, order placement, cancellation, investment advice, strategy scoring, alerting, backtesting, and historical warehousing.
- CFFEX, stock options, ETF options, and unapproved paid vendor integrations.
- Storing real credentials or full API keys in tracked files or evidence.

## Runtime Rules

- Use `uv` for development and verification.
- TQSDK secrets are stored only in the local runtime SQLite database with Windows DPAPI protection where available.
- QA and sample databases must not contain real TQSDK passwords or full API keys.

