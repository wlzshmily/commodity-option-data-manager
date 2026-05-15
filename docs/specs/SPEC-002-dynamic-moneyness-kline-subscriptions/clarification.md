# Clarification

## Confirmed Decisions

| Date | Decision | Impact |
| --- | --- | --- |
| 2026-05-14 | Contract-list management and moneyness filtering are separate. Contract-list changes are low-frequency; moneyness filtering is intraday. | The feature will not overload the contract manager with tick-level responsibilities. |
| 2026-05-14 | Quote resources can remain broadly subscribed for the selected contract-month scope. | T型报价 remains complete, and moneyness can be evaluated from current underlying prices without narrowing Quote rows. |
| 2026-05-14 | Kline resources are filtered by selected moneyness ranges. | The performance savings target the expensive subscription type. |
| 2026-05-14 | Moneyness recalculation defaults to a 30-second interval. | The worker gets predictable periodic checks instead of per-tick resubscription churn. |
| 2026-05-14 | Intraday Kline filtering is sticky: add newly matching Klines but do not release old ones until restart/new session/full scope rebuild. | This removes the main stability risk from ATM boundary churn. |
| 2026-05-14 | Trading-session checks must be contract/product-granular, not exchange-granular. | Products on the same exchange can differ in night-session support. |
| 2026-05-14 | WebUI contract lists and T型报价 should show whether the contract/chain is in trading time. | Session Manager becomes user-visible, not only an internal scheduler helper. |

## No Open Blockers

The current requirements have no unresolved clarification blocker. Implementation may proceed after the task plan is accepted under the project SDLC controls.
