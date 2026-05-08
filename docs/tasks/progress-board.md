# Progress Board

## In Progress

- Bounded live full-market acceptance windows

## Pending

- Final full-market completion report
- Full-market batch threshold tuning from complete coverage evidence

## Done

- Requirements reviewed and implementation plan approved.
- Package structure and `pyproject.toml`.
- Governance baseline.
- Unified local API and API Key management.
- WebUI mounted local API settings/key/refresh endpoints.
- Test harness and smoke validation.
- `odm-collect --max-batches` bounded window control.
- WebUI/API collection batch progress visibility.
- Background refresh worker for continuous bounded full-market collection.
- Metric-only Greeks/IV gaps are treated as visible quality gaps instead of batch blockers.
- Overview time columns now distinguish exchange market time from collector update time.
- Background collection wait cycles are configurable and default to one wait round.
