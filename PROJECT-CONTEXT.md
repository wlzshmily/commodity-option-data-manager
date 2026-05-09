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

- Use `uv` for development and verification; local development now targets WSL2 Ubuntu first.
- TQSDK secrets are stored only in local runtime state: Windows uses DPAPI, and WSL/Linux uses a per-user Fernet key file.
- QA and sample databases must not contain real TQSDK passwords or full API keys.

## Current Release Evidence

- Local direct TQSDK acceptance completed on 2026-05-09 with `odm-collect-parallel`: 864/864 full-market batches succeeded, 0 failed, 27,386 active option symbols covered.
- Realtime Quote freshness is delivered by the quote-only `odm-quote-stream` worker; full-market catch-up is delivered by process-level shards.
- Remaining source gaps in IV/Greeks or empty price fields are exposed as data-quality diagnostics rather than treated as collection failures.
