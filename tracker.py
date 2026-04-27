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
                    alert = {
                        "trader": name,
                        "wallet": wallet,
                        "side": trade.get("side", "???"),
                        "outcome": trade.get("outcome", "???"),
                        "title": sport_info.get("title", trade.get("title", "Unknown")),
                        "sport": sport_info.get("sport", "Sports"),
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
    <title>Polymarket Whale Tracker</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg: #111;
            --surface: #1a1a1a;
            --border: #2a2a2a;
            --border-hover: #444;
            --text: #ddd;
            --text-dim: #666;
            --text-muted: #444;
            --green: #3d9970;
            --red: #c0392b;
            --mono: 'Courier New', Courier, monospace;
        }

        body {
            background: var(--bg);
            color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            font-size: 14px;
            min-height: 100vh;
        }

        .container {
            max-width: 860px;
            margin: 0 auto;
            padding: 32px 20px;
        }

        .header {
            margin-bottom: 28px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 20px;
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 12px;
        }

        .header h1 {
            font-size: 15px;
            font-weight: 600;
            color: var(--text);
            letter-spacing: 0.01em;
        }

        .header p {
            color: var(--text-dim);
            font-size: 12px;
        }

        .status-bar {
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
            margin-bottom: 24px;
            font-family: var(--mono);
            font-size: 11px;
            color: var(--text-dim);
        }

        .status-item {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .status-item .dot {
            width: 5px;
            height: 5px;
            border-radius: 50%;
            background: var(--green);
            animation: pulse 2.5s ease-in-out infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.25; }
        }

        .status-item .val {
            color: var(--text);
        }

        .feed-label {
            font-family: var(--mono);
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            color: var(--text-muted);
            margin-bottom: 10px;
        }

        .feed {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .trade-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-left: 2px solid transparent;
            padding: 12px 16px;
            display: grid;
            grid-template-columns: 52px 1fr auto;
            gap: 14px;
            align-items: center;
            text-decoration: none;
            color: inherit;
            transition: border-color 0.15s;
            animation: fadeSlide 0.25s ease-out;
        }

        .trade-card:hover {
            border-color: var(--border-hover);
        }

        @keyframes fadeSlide {
            from { opacity: 0; transform: translateY(-8px); }
            to   { opacity: 1; transform: translateY(0); }
        }

        .trade-card.buy  { border-left-color: var(--green); }
        .trade-card.sell { border-left-color: var(--red); }

        .sport-tag {
            font-family: var(--mono);
            font-size: 10px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: var(--text-dim);
            background: var(--bg);
            border: 1px solid var(--border);
            padding: 3px 6px;
            text-align: center;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .trade-info { min-width: 0; }

        .trade-market {
            font-size: 13px;
            font-weight: 500;
            margin-bottom: 4px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            color: var(--text);
        }

        .trade-meta {
            font-family: var(--mono);
            font-size: 11px;
            color: var(--text-dim);
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }

        .trade-meta .trader { color: #888; }
        .trade-meta .side-buy  { color: var(--green); }
        .trade-meta .side-sell { color: var(--red); }

        .trade-right {
            text-align: right;
            white-space: nowrap;
        }

        .trade-right .amount {
            font-family: var(--mono);
            font-weight: 600;
            font-size: 13px;
        }

        .trade-right .amount.buy  { color: var(--green); }
        .trade-right .amount.sell { color: var(--red); }

        .trade-right .price,
        .trade-right .time {
            font-family: var(--mono);
            font-size: 10px;
            color: var(--text-dim);
            margin-top: 2px;
        }

        .empty-state {
            padding: 60px 20px;
            text-align: center;
            color: var(--text-muted);
            font-family: var(--mono);
            font-size: 12px;
        }

        .sound-toggle {
            position: fixed;
            top: 14px;
            right: 16px;
            background: var(--surface);
            border: 1px solid var(--border);
            padding: 5px 10px;
            color: var(--text-dim);
            font-family: var(--mono);
            font-size: 10px;
            cursor: pointer;
            transition: border-color 0.15s, color 0.15s;
        }

        .sound-toggle:hover { border-color: var(--border-hover); color: var(--text); }
        .sound-toggle.active { border-color: var(--green); color: var(--green); }
    </style>
</head>
<body>
    <button class="sound-toggle" id="soundToggle" onclick="toggleSound()">Sound: off</button>

    <div class="container">
        <div class="header">
            <h1>Polymarket Sports Whale Tracker</h1>
            <p>real-time large bets on sports markets</p>
        </div>

        <div class="status-bar">
            <div class="status-item">
                <span class="dot"></span>
                <span>live</span>
            </div>
            <div class="status-item">
                whales: <span class="val" id="whaleCount">--</span>
            </div>
            <div class="status-item">
                markets: <span class="val" id="marketCount">--</span>
            </div>
            <div class="status-item">
                alerts: <span class="val" id="alertCount">0</span>
            </div>
            <div class="status-item">
                polled: <span class="val" id="lastPoll">--</span>
            </div>
        </div>

        <div class="feed-label">Feed</div>

        <div class="feed" id="feed">
            <div class="empty-state" id="emptyState">Waiting for trades...</div>
        </div>
    </div>

    <script>
        let soundEnabled = false;
        let alertCount = 0;

        function toggleSound() {
            soundEnabled = !soundEnabled;
            const btn = document.getElementById('soundToggle');
            btn.textContent = soundEnabled ? 'Sound: on' : 'Sound: off';
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

            const sportAbbr = (trade.sport || 'SPT').slice(0, 6).toUpperCase();

            card.innerHTML = `
                <div class="sport-tag">${sportAbbr}</div>
                <div class="trade-info">
                    <div class="trade-market">${trade.title}</div>
                    <div class="trade-meta">
                        <span class="trader">${trade.trader}</span>
                        <span class="side-${side}">${trade.side} ${trade.outcome}</span>
                    </div>
                </div>
                <div class="trade-right">
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

            while (feed.children.length > 50) {
                feed.removeChild(feed.lastChild);
            }

            alertCount++;
            document.getElementById('alertCount').textContent = alertCount;
            document.title = `(${alertCount}) Whale Tracker`;

            playAlert();
        }

        function connect() {
            const evtSource = new EventSource('/stream');

            evtSource.addEventListener('trade', function(e) {
                addTrade(JSON.parse(e.data));
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

    app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)