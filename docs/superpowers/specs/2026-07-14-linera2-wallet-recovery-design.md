# Linera 2.0 Wallet Recovery Design

## Goal

Add unattended OKX wallet recovery to Linera 2.0 without changing the default
read-only readiness scan. Wallet recovery runs only when Auto mode is explicitly
enabled and the first readiness result is `wallet_disconnected`.

## Chosen Approach

Create a focused `linera2.wallet_recovery` adapter. It reuses the proven parent
module primitives `unlock_okx_wallet()` and `_click_wallet_button()` while owning
the new Linera UI integration:

- locate the `Connect` button on the current Linera page;
- open the Dynamic login modal;
- find the OKX wallet tile inside the open Shadow DOM;
- derive the live element bounds and issue a trusted Playwright mouse click;
- detect the OKX `notification.html` page regardless of its current hash route;
- let the parent confirmation helper process the three `dapp-entry` confirmations;
- wait until the Linera wallet button shows a masked address and the Connect button
  is gone.

The old `linera_task.reconnect_wallet()` is not called because its site selectors
target the previous UI. The parent password, unlock, and confirmation logic is not
copied into Linera 2.0.

## Public Interface

```python
@dataclass(frozen=True)
class WalletRecoveryResult:
    recovered: bool
    reason: str


async def ensure_wallet_connected(
    page: Page,
    context: BrowserContext,
    account_id: str,
    *,
    timeout: int = 90,
    log_func: Callable[[str, str], None] | None = None,
) -> WalletRecoveryResult:
    ...
```

Parent functions are loaded lazily so importing Linera 2.0 does not execute the
old task flow or introduce a second copy of wallet secrets.

## Runtime Flow

1. Start HubStudio and connect through CDP as today.
2. Run the existing read-only `check_account_ready()` once.
3. If Auto mode is disabled, store and return that result unchanged.
4. If Auto mode is enabled and state is not `wallet_disconnected`, do not invoke
   wallet recovery.
5. If Auto mode is enabled and state is `wallet_disconnected`, call
   `ensure_wallet_connected()` once.
6. Run `check_account_ready()` again after the recovery attempt. This second
   readiness result is the only result stored and used by Auto.
7. Start Auto only when the second result is `ready=True`.

There is no page refresh, browser restart, repeated wallet recovery loop, or Auto
start inside the recovery adapter.

## Recovery Details

- Call `unlock_okx_wallet()` before opening the site connection modal. A false or
  exceptional result fails recovery without clicking Connect.
- Close only a stale Dynamic login modal owned by the Linera page, then open a new
  modal through the visible `Connect` button.
- The OKX tile must be found by `data-testid="ListTile"` plus text containing
  `OKX Wallet`. Coordinates are read from `getBoundingClientRect()` at click time;
  no fixed screen coordinates are stored.
- Register popup observation before the trusted click so a fast notification page
  is not missed.
- Match only the configured OKX extension's `notification.html` page. Ignore
  MetaMask and offscreen extension pages.
- Do not trust the initial popup hash. The route can change from `dapp-read` to
  `unlock` or `dapp-entry`; confirmation operates on the current page contents.
- A successful result requires the existing frontend snapshot reader to report a
  connected masked wallet. Popup disappearance alone is not success.

## Failure Behaviour

Expected failures return `WalletRecoveryResult(False, reason)` and do not escape as
unhandled exceptions. Reasons are concise and may identify these stages:

- parent wallet unlock failed;
- Connect button unavailable;
- Dynamic modal or OKX tile unavailable;
- wallet notification page unavailable;
- confirmation timed out;
- wallet button did not become connected.

After any failure, runtime reruns readiness. The final public state remains one of
the existing readiness states, normally `wallet_disconnected` or
`wallet_syncing`; no new readiness enum is added.

## Privacy and Safety

- Never log the wallet password, cookie, authorization header, request body,
  response body, or full wallet address.
- Reuse the parent password constant only inside the parent unlock helper.
- Default readiness mode stays read-only.
- Recovery is opt-in through `--auto-session` and may only connect/sign the wallet;
  it does not configure or start Auto.
- Recovery is attempted at most once per account scan.

## Tests

- Already connected: no parent unlock call and no click.
- Parent unlock failure: recovery fails before Connect.
- Collapsed Dynamic modal: Connect opens, Shadow DOM OKX tile receives a trusted
  mouse click, and popup observation is installed first.
- Stale Dynamic modal: it is closed before reopening Connect.
- New `dapp-entry` popup: three confirmation steps are delegated to the parent
  confirmation helper and connected state is required afterward.
- Popup route changes: matching does not depend on the first hash route.
- Auto disabled plus disconnected: recovery is never called.
- Auto enabled plus disconnected: recovery runs once, readiness runs twice, and
  Auto receives only the second ready result.
- Recovery failure or second readiness failure: Auto never starts.
- Sensitive-value scan finds no password, full wallet address, Cookie, or
  Authorization data in status files or logs.

## Live Verification

Use HubStudio environment `625421671` with Auto disabled for the recovery check.
Confirm that a closed/reopened DApp session can be unlocked and connected, that the
three login confirmations complete, and that the final state is ready. Do not start
Auto or place a bet during the wallet-only verification.
