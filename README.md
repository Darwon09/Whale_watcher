# Polymarket Whale Tracker

Monitors the top sports bettors on Polymarket and streams their trades in real time via a local web dashboard.

## How it works

- Pulls the top traders from the Polymarket leaderboard (filtered to sports)
- Indexes active sports markets using the Gamma API
- Polls each trader's recent trades and surfaces any that hit a minimum size threshold
- Pushes alerts to a browser dashboard over SSE (Server-Sent Events)

## Setup

```bash
pip install flask requests python-dotenv
```

Create a `.env` file in the project root:

```
POLL_INTERVAL=30
MIN_TRADE_SIZE=1000
TOP_N_TRADERS=50
SPORTS_FILTER=NBA,NFL,UFC,MLB,GOLF,NHL
```

| Variable | Description | Default |
|---|---|---|
| `POLL_INTERVAL` | Seconds between polling each trader | `30` |
| `MIN_TRADE_SIZE` | Minimum trade size in USDC to show | `100` |
| `TOP_N_TRADERS` | How many leaderboard traders to track | `15` |
| `SPORTS_FILTER` | Comma-separated sports to watch (leave blank for all) | all |

## Run

```bash
python tracker.py
```

Then open `http://localhost:8000` in your browser.

## Notes

- Trade history is saved to `trades_history.json` and loaded on restart
- The sports market index refreshes every 100 poll cycles
- Clicking a trade card opens the market on Polymarket
