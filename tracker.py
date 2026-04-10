"""
Polymarket Sports Whale Tracker — Live Dashboard
==================================================
Runs a local web server with a live-updating dashboard
that shows whale sports bets as they happen.

Setup:
  pip install requests flask python-dotenv
  python sports_dashboard.py

Then open http://localhost:5000 in your browser.
"""

import os
import time
import json
import threading
import queue
import requests
from datetime import datetime, timezone
from flask import Flask, Response, render_template_string
from dotenv import load_dotenv

load_dotenv()


DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
MIN_TRADE_SIZE = float(os.getenv("MIN_TRADE_SIZE", "100"))
TOP_N_TRADERS = int(os.getenv("TOP_N_TRADERS", "15"))
SPORTS_FILTER = os.getenv("SPORTS_FILTER", "")

TRADES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades_history.json")
MAX_RECENT = 50


sse_subscribers = []
sse_lock = threading.Lock()

recent_trades = []

tracker_status = {
    "whales": 0,
    "sports_markets": 0,
    "total_alerts": 0,
    "last_poll": "Starting...",
    "sports_tracked": [],
}


def load_trades_from_disk():
    global recent_trades
    try:
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE, "r") as f:
                recent_trades = json.load(f)
                print(f"[*] Loaded {len(recent_trades)} trades from disk.")
    except Exception as e:
        print(f"[WARN] Could not load trades file: {e}")
        recent_trades = []


def save_trades_to_disk():
    try:
        with open(TRADES_FILE, "w") as f:
            json.dump(recent_trades, f)
    except Exception as e:
        print(f"[WARN] Could not save trades file: {e}")


def broadcast_trade(alert):
    """Push a trade to all connected browser tabs."""
    recent_trades.insert(0, alert)
    del recent_trades[MAX_RECENT:]
    tracker_status["total_alerts"] += 1
    save_trades_to_disk()

    with sse_lock:
        dead = []
        for q in sse_subscribers:
            try:
                q.put_nowait(alert)
            except queue.Full:
                dead.append(q)
        for q in dead:
            sse_subscribers.remove(q)

def get_sports_metadata():
    url = f"{GAMMA_API}/sports"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Sports metadata: {e}")
        return []


def get_sports_events(tag_id, limit=100):
    url = f"{GAMMA_API}/events"
    params = {"tag_id": tag_id, "active": "true", "closed": "false", "limit": limit}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Events for tag {tag_id}: {e}")
        return []


def build_sports_market_index():
    sports = get_sports_metadata()
    if not sports:
        return {}, set()

    filter_sports = set()
    if SPORTS_FILTER:
        filter_sports = {s.strip().lower() for s in SPORTS_FILTER.split(",")}

    sport_tags = {}
    for sport in sports:
        sport_name = sport.get("sport", "Unknown")
        tags_str = sport.get("tags", "")
        if filter_sports and sport_name.lower() not in filter_sports:
            continue
        if tags_str:
            for tag_id in str(tags_str).split(","):
                tag_id = tag_id.strip()
                if tag_id:
                    sport_tags[tag_id] = sport_name

    sports_markets = {}
    sports_condition_ids = set()
    tracked_sports = set()

    for tag_id, sport_name in sport_tags.items():
        tracked_sports.add(sport_name)
        events = get_sports_events(tag_id)
        for event in events:
            event_title = event.get("title", "")
            event_slug = event.get("slug", "")
            markets = event.get("markets", [])
            for market in markets:
                cond_id = market.get("conditionId", "")
                if cond_id:
                    sports_markets[cond_id] = {
                        "sport": sport_name,
                        "title": market.get("question", event_title),
                        "slug": market.get("slug", ""),
                        "eventSlug": event_slug,
                    }
                    sports_condition_ids.add(cond_id)
        time.sleep(0.3)

    tracker_status["sports_markets"] = len(sports_markets)
    tracker_status["sports_tracked"] = list(tracked_sports)
    return sports_markets, sports_condition_ids


def get_leaderboard(limit=15):
    url = f"{DATA_API}/v1/leaderboard"
    params = {"timePeriod": "ALL", "orderBy": "PNL", "category": "SPORTS", "limit": limit}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Leaderboard: {e}")
        return []


def get_trades_for_user(wallet_address, limit=20):
    url = f"{DATA_API}/trades"
    params = {"user": wallet_address, "limit": limit}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return []


def tracker_loop():
    global recent_trades

    print("[Tracker] Building sports market index...")
    sports_markets, sports_condition_ids = build_sports_market_index()
    print(f"[Tracker] Indexed {len(sports_condition_ids)} sports markets")

    print("[Tracker] Fetching leaderboard...")
    leaderboard = get_leaderboard(limit=TOP_N_TRADERS)

    whales = {}
    for entry in leaderboard:
        wallet = entry.get("proxyWallet") or entry.get("address", "")
        name = entry.get("userName") or entry.get("pseudonym") or entry.get("name") or wallet[:10]
        if wallet:
            whales[wallet] = name

    tracker_status["whales"] = len(whales)
    print(f"[Tracker] Tracking {len(whales)} whales")

    seen_trades = set()
    for wallet in whales:
        trades = get_trades_for_user(wallet, limit=5)
        for t in trades:
            tx = t.get("transactionHash", "")
            if tx:
                seen_trades.add(tx)
        time.sleep(0.3)

    print(f"[Tracker] Loaded {len(seen_trades)} existing trades. Monitoring...")
    refresh_counter = 0

    while True:
        print("Running")
        try:
            refresh_counter += 1
            if refresh_counter >= 100:
                sports_markets, sports_condition_ids = build_sports_market_index()
                refresh_counter = 0

            for wallet, name in whales.items():
                trades = get_trades_for_user(wallet, limit=10)
                for trade in trades:
                    tx = trade.get("transactionHash", "")
                    if not tx or tx in seen_trades:
                        print("no new trade")
                        continue
                    seen_trades.add(tx)

                    cond_id = trade.get("conditionId", "")
                    if cond_id not in sports_condition_ids:
                        continue

                    usdc_size = trade.get("usdcSize") or (
                        trade.get("size", 0) * trade.get("price", 0)
                    )
                    if not usdc_size or usdc_size < MIN_TRADE_SIZE:
                        continue

                    sport_info = sports_markets.get(cond_id, {})
                    sport_emojis = {
                        "NBA": "🏀", "NFL": "🏈", "MLB": "⚾", "NHL": "🏒",
                        "Soccer": "⚽", "UFC": "🥊", "MMA": "🥊", "Tennis": "🎾",
                        "F1": "🏎️", "Golf": "⛳",
                    }

                    alert = {
                        "trader": name,
                        "wallet": wallet,
                        "side": trade.get("side", "???"),
                        "outcome": trade.get("outcome", "???"),
                        "title": sport_info.get("title", trade.get("title", "Unknown")),
                        "sport": sport_info.get("sport", "Sports"),
                        "sportEmoji": sport_emojis.get(sport_info.get("sport", ""), "🏆"),
                        "size": round(usdc_size, 2),
                        "price": round(trade.get("price", 0), 4),
                        "eventSlug": sport_info.get("eventSlug", ""),
                        "tx": tx,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

                    broadcast_trade(alert)
                    print(f"[ALERT] {name} {alert['side']} on {alert['sport']}: {alert['title']} (${usdc_size:,.2f})")

                time.sleep(0.3)

            tracker_status["last_poll"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
            time.sleep(POLL_INTERVAL)

        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(5)

app = Flask(__name__)

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Polymarket Sports Whale Tracker</title>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg: #0a0a0f;
            --surface: #12121a;
            --surface-2: #1a1a25;
            --border: #2a2a3a;
            --text: #e8e8f0;
            --text-dim: #7a7a8e;
            --green: #00e676;
            --green-dim: rgba(0, 230, 118, 0.1);
            --red: #ff5252;
            --red-dim: rgba(255, 82, 82, 0.1);
            --accent: #7c4dff;
            --accent-dim: rgba(124, 77, 255, 0.1);
            --gold: #ffd740;
        }

        body {
            background: var(--bg);
            color: var(--text);
            font-family: 'Space Grotesk', sans-serif;
            min-height: 100vh;
            overflow-x: hidden;
        }

        /* Background grid effect */
        body::before {
            content: '';
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background-image:
                linear-gradient(rgba(124, 77, 255, 0.03) 1px, transparent 1px),
                linear-gradient(90deg, rgba(124, 77, 255, 0.03) 1px, transparent 1px);
            background-size: 60px 60px;
            pointer-events: none;
            z-index: 0;
        }

        .container {
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 20px;
            position: relative;
            z-index: 1;
        }

        /* Header */
        .header {
            text-align: center;
            margin-bottom: 40px;
        }

        .header h1 {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.6rem;
            font-weight: 700;
            letter-spacing: -0.5px;
            margin-bottom: 8px;
        }

        .header h1 .whale { color: var(--accent); }

        .header p {
            color: var(--text-dim);
            font-size: 0.85rem;
            font-family: 'JetBrains Mono', monospace;
        }

        /* Status bar */
        .status-bar {
            display: flex;
            gap: 12px;
            justify-content: center;
            flex-wrap: wrap;
            margin-bottom: 32px;
        }

        .status-chip {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 8px 16px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.75rem;
            color: var(--text-dim);
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .status-chip .dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: var(--green);
            animation: pulse 2s ease-in-out infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        .status-chip .value {
            color: var(--text);
            font-weight: 600;
        }

        /* Trade feed */
        .feed-header {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 16px;
            padding-left: 4px;
        }

        .feed {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .trade-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 16px 20px;
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 16px;
            align-items: center;
            transition: all 0.3s ease;
            animation: slideIn 0.4s ease-out;
            cursor: pointer;
            text-decoration: none;
            color: inherit;
        }

        .trade-card:hover {
            border-color: var(--accent);
            background: var(--surface-2);
            transform: translateY(-1px);
        }

        @keyframes slideIn {
            from {
                opacity: 0;
                transform: translateY(-20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .trade-card.buy { border-left: 3px solid var(--green); }
        .trade-card.sell { border-left: 3px solid var(--red); }

        .trade-sport {
            font-size: 1.8rem;
            line-height: 1;
        }

        .trade-info {
            min-width: 0;
        }

        .trade-market {
            font-weight: 600;
            font-size: 0.9rem;
            margin-bottom: 4px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .trade-meta {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.72rem;
            color: var(--text-dim);
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
        }

        .trade-meta .trader { color: var(--accent); }
        .trade-meta .side-buy { color: var(--green); font-weight: 600; }
        .trade-meta .side-sell { color: var(--red); font-weight: 600; }

        .trade-size {
            text-align: right;
            white-space: nowrap;
        }

        .trade-size .amount {
            font-family: 'JetBrains Mono', monospace;
            font-weight: 700;
            font-size: 1rem;
        }

        .trade-size .amount.buy { color: var(--green); }
        .trade-size .amount.sell { color: var(--red); }

        .trade-size .price {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.7rem;
            color: var(--text-dim);
        }

        .trade-size .time {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.65rem;
            color: var(--text-dim);
            margin-top: 2px;
        }

        /* Empty state */
        .empty-state {
            text-align: center;
            padding: 80px 20px;
            color: var(--text-dim);
        }

        .empty-state .icon {
            font-size: 3rem;
            margin-bottom: 16px;
            opacity: 0.5;
        }

        .empty-state p {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
        }

        .empty-state .sub {
            font-size: 0.75rem;
            margin-top: 8px;
            opacity: 0.6;
        }

        /* New trade flash */
        .trade-card.new {
            animation: flashIn 0.6s ease-out;
        }

        @keyframes flashIn {
            0% {
                opacity: 0;
                transform: translateY(-30px) scale(0.98);
                box-shadow: 0 0 30px rgba(124, 77, 255, 0.3);
            }
            50% {
                box-shadow: 0 0 20px rgba(124, 77, 255, 0.2);
            }
            100% {
                opacity: 1;
                transform: translateY(0) scale(1);
                box-shadow: none;
            }
        }

        /* Sound toggle */
        .sound-toggle {
            position: fixed;
            top: 16px;
            right: 16px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 8px 12px;
            color: var(--text-dim);
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.7rem;
            cursor: pointer;
            z-index: 10;
            transition: border-color 0.2s;
        }

        .sound-toggle:hover { border-color: var(--accent); }
        .sound-toggle.active { border-color: var(--green); color: var(--green); }
    </style>
</head>
<body>
    <button class="sound-toggle" id="soundToggle" onclick="toggleSound()">🔇 Sound Off</button>

    <div class="container">
        <div class="header">
            <h1>🐋 <span class="whale">Polymarket</span> Sports Whale Tracker</h1>
            <p>real-time whale alerts on sports bets</p>
        </div>

        <div class="status-bar" id="statusBar">
            <div class="status-chip">
                <span class="dot"></span>
                <span>Live</span>
            </div>
            <div class="status-chip">
                Whales: <span class="value" id="whaleCount">...</span>
            </div>
            <div class="status-chip">
                Markets: <span class="value" id="marketCount">...</span>
            </div>
            <div class="status-chip">
                Alerts: <span class="value" id="alertCount">0</span>
            </div>
            <div class="status-chip">
                Last poll: <span class="value" id="lastPoll">...</span>
            </div>
        </div>

        <div class="feed-header">Live Feed</div>

        <div class="feed" id="feed">
            <div class="empty-state" id="emptyState">
                <div class="icon">🎯</div>
                <p>Watching for whale sports bets...</p>
                <p class="sub">New trades will appear here in real-time</p>
            </div>
        </div>
    </div>

    <script>
        let soundEnabled = false;
        let alertCount = 0;

        function toggleSound() {
            soundEnabled = !soundEnabled;
            const btn = document.getElementById('soundToggle');
            btn.textContent = soundEnabled ? '🔊 Sound On' : '🔇 Sound Off';
            btn.classList.toggle('active', soundEnabled);
        }

        function playAlert() {
            if (!soundEnabled) return;
            try {
                const ctx = new (window.AudioContext || window.webkitAudioContext)();
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.frequency.value = 800;
                osc.type = 'sine';
                gain.gain.setValueAtTime(0.15, ctx.currentTime);
                gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
                osc.start(ctx.currentTime);
                osc.stop(ctx.currentTime + 0.3);
            } catch(e) {}
        }

        function formatTime(isoStr) {
            const d = new Date(isoStr);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }

        function createTradeCard(trade, isNew) {
            const side = trade.side.toLowerCase();
            const link = trade.eventSlug
                ? `https://polymarket.com/event/${trade.eventSlug}`
                : '#';

            const card = document.createElement('a');
            card.href = link;
            card.target = '_blank';
            card.rel = 'noopener';
            card.className = `trade-card ${side}${isNew ? ' new' : ''}`;

            card.innerHTML = `
                <div class="trade-sport">${trade.sportEmoji}</div>
                <div class="trade-info">
                    <div class="trade-market">${trade.title}</div>
                    <div class="trade-meta">
                        <span class="trader">${trade.trader}</span>
                        <span class="side-${side}">${trade.side} ${trade.outcome}</span>
                        <span>${trade.sport}</span>
                    </div>
                </div>
                <div class="trade-size">
                    <div class="amount ${side}">$${trade.size.toLocaleString()}</div>
                    <div class="price">@ ${trade.price}</div>
                    <div class="time">${formatTime(trade.timestamp)}</div>
                </div>
            `;

            return card;
        }

        function addTrade(trade) {
            const feed = document.getElementById('feed');
            const empty = document.getElementById('emptyState');
            if (empty) empty.remove();

            const card = createTradeCard(trade, true);
            feed.insertBefore(card, feed.firstChild);

            // Cap at 50
            while (feed.children.length > 50) {
                feed.removeChild(feed.lastChild);
            }

            alertCount++;
            document.getElementById('alertCount').textContent = alertCount;
            document.title = `(${alertCount}) Whale Tracker`;

            playAlert();
        }

        // SSE connection
        function connect() {
            const evtSource = new EventSource('/stream');

            evtSource.addEventListener('trade', function(e) {
                const trade = JSON.parse(e.data);
                addTrade(trade);
            });

            evtSource.addEventListener('status', function(e) {
                const s = JSON.parse(e.data);
                document.getElementById('whaleCount').textContent = s.whales;
                document.getElementById('marketCount').textContent = s.sports_markets;
                document.getElementById('lastPoll').textContent = s.last_poll;
            });

            evtSource.onerror = function() {
                evtSource.close();
                setTimeout(connect, 3000);
            };
        }

        // Load existing trades on page load
        fetch('/api/trades')
            .then(r => r.json())
            .then(data => {
                if (data.trades.length > 0) {
                    const empty = document.getElementById('emptyState');
                    if (empty) empty.remove();

                    const feed = document.getElementById('feed');
                    data.trades.forEach(trade => {
                        feed.appendChild(createTradeCard(trade, false));
                    });
                    alertCount = data.trades.length;
                    document.getElementById('alertCount').textContent = alertCount;
                }

                document.getElementById('whaleCount').textContent = data.status.whales;
                document.getElementById('marketCount').textContent = data.status.sports_markets;
                document.getElementById('lastPoll').textContent = data.status.last_poll;
            })
            .catch(() => {});

        connect();
    </script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/trades")
def api_trades():
    return json.dumps({"trades": recent_trades, "status": tracker_status})


@app.route("/stream")
def stream():
    def event_stream():
        q = queue.Queue(maxsize=100)
        with sse_lock:
            sse_subscribers.append(q)
        try:
            while True:
                try:
                    trade = q.get(timeout=10)
                    yield f"event: trade\ndata: {json.dumps(trade)}\n\n"
                except queue.Empty:
                    yield f"event: status\ndata: {json.dumps(tracker_status)}\n\n"
        finally:
            with sse_lock:
                if q in sse_subscribers:
                    sse_subscribers.remove(q)

    return Response(event_stream(), mimetype="text/event-stream")



if __name__ == "__main__":
    load_trades_from_disk()
    tracker_thread = threading.Thread(target=tracker_loop, daemon=True)
    tracker_thread.start()

    print("\n[*] Dashboard running at http://localhost:5000\n")
    app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)