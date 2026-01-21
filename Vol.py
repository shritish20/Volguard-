#!/usr/bin/env python3
"""
VOLGUARD 3.2 – Option Seller Cockpit
Production-ready with WebSocket Greeks, Multi-Trade Recovery, Emergency Flatten
"""
import os, sys, time, json, sqlite3, logging, threading, multiprocessing, traceback, signal, atexit, concurrent.futures, requests, pandas as pd, numpy as np, pytz, psutil, yfinance as yf
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
from urllib.parse import quote, urlparse, parse_qs
from contextlib import contextmanager
import io, queue, hashlib, base64, smtplib, yaml, uuid
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from http.server import HTTPServer, BaseHTTPRequestHandler
from cryptography.fernet import Fernet
import xml.etree.ElementTree as ET
from groq import Groq
from arch import arch_model
from logging.handlers import RotatingFileHandler

import upstox_client
from upstox_client.rest import ApiException
from upstox_client import OrderApiV3
from upstox_client.api.history_v3_api import HistoryV3Api
from upstox_client.api.options_api import OptionsApi
from upstox_client.api.login_api import LoginApi
from upstox_client.api.charge_api import ChargeApi
from upstox_client.api.market_holidays_and_timings_api import MarketHolidaysAndTimingsApi
from upstox_client.api.order_api import OrderApi
from upstox_client.api.portfolio_api import PortfolioApi
# ----------  NEW: WEBSOCKET GREEKS  ----------
from upstox_client.api.market_data_streamer_v3 import MarketDataStreamerV3

# ----------  PROMETHEUS  ----------
from prometheus_client import start_http_server, Counter, Histogram, Gauge, CollectorRegistry
PROM_REGISTRY = CollectorRegistry()
sys_uptime = Gauge("volguard_uptime_seconds", "Seconds since VolGuard started", registry=PROM_REGISTRY)
sys_uptime.set_to_current_time()
trade_counter = Counter("volguard_trades_total", "Trades opened by strategy and expiry", ["strategy", "expiry_type"], registry=PROM_REGISTRY)
trade_pnl = Gauge("volguard_trade_pnl_inr", "Real-time P&L per trade_id", ["trade_id", "strategy"], registry=PROM_REGISTRY)
order_exec_latency = Histogram("volguard_order_latency_seconds", "Time between place_order and final fill/cancel", ["side", "role"], buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0), registry=PROM_REGISTRY)
order_fill_ratio = Gauge("volguard_order_fill_ratio", "Filled qty / requested qty", ["instrument_key", "side", "role"], registry=PROM_REGISTRY)
slippage_pct = Histogram("volguard_slippage_percent", "abs(actual_price – expected_price)/expected_price * 100", ["instrument_key", "side"], buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.5), registry=PROM_REGISTRY)
order_timeout_counter = Counter("volguard_order_timeouts_total", "Orders that timed out", ["side", "role"], registry=PROM_REGISTRY)
greeks_delta = Gauge("volguard_portfolio_delta", "Net portfolio delta", registry=PROM_REGISTRY)
greeks_theta = Gauge("volguard_portfolio_theta", "Net portfolio theta", registry=PROM_REGISTRY)
greeks_gamma = Gauge("volguard_portfolio_gamma", "Net portfolio gamma", registry=PROM_REGISTRY)
greeks_vega = Gauge("volguard_portfolio_vega", "Net portfolio vega", registry=PROM_REGISTRY)
margin_used_gauge = Gauge("volguard_margin_used_percent", "Used margin / available margin * 100", registry=PROM_REGISTRY)
iv_rank_weekly = Gauge("volguard_iv_rank_weekly", "Weekly IV percentile vs 1-year", registry=PROM_REGISTRY)
iv_rank_monthly = Gauge("volguard_iv_rank_monthly", "Monthly IV percentile vs 1-year", registry=PROM_REGISTRY)
vov_zscore = Gauge("volguard_vov_zscore", "Vol-of-Vol z-score", registry=PROM_REGISTRY)
price_staleness_sec = Gauge("volguard_price_staleness_seconds", "Seconds since last LTP update", registry=PROM_REGISTRY)
circuit_breaker_active = Gauge("volguard_circuit_breaker_active", "1 = breaker open (no new trades)", registry=PROM_REGISTRY)
db_queue_size = Gauge("volguard_db_queue_size", "Outstanding writes in DB writer queue", registry=PROM_REGISTRY)

def record_trade_open(strategy: str, expiry_type: str, trade_id: str):
    trade_counter.labels(strategy=strategy, expiry_type=expiry_type).inc()
def record_order_fill(leg: dict, elapsed: float):
    role, side, key = leg.get("role", "UNKNOWN").upper(), leg["side"].upper(), leg["key"]
    req_qty, filled = leg["qty"], leg.get("filled_qty", 0)
    ratio = filled / req_qty if req_qty else 0
    order_fill_ratio.labels(instrument_key=key, side=side, role=role).set(ratio)
    order_exec_latency.labels(side=side, role=role).observe(elapsed)
def record_slippage(leg: dict):
    entry, expected = leg.get("entry_price", 0.0), leg.get("ltp", entry)
    if expected > 0:
        pct = abs(entry - expected) / expected * 100
        slippage_pct.labels(instrument_key=leg["key"], side=leg["side"].upper()).observe(pct)
def update_greeks(delta, theta, gamma, vega):
    greeks_delta.set(delta); greeks_theta.set(theta); greeks_gamma.set(gamma); greeks_vega.set(vega)
def start_metrics_server(port=8000):
    prometheus_client.REGISTRY = PROM_REGISTRY
    start_http_server(port, registry=PROM_REGISTRY)
    logger.info(f"📊 Prometheus metrics served on 0.0.0.0:{port}/metrics")

# ----------  CONFIG  ----------
class ProductionConfig:
    ENVIRONMENT = os.getenv("VG_ENV", "PRODUCTION")
    DRY_RUN_MODE = os.getenv("VG_DRY_RUN", "FALSE").upper() == "TRUE"
    UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    UPSTOX_CLIENT_ID = os.getenv("UPSTOX_CLIENT_ID")
    UPSTOX_CLIENT_SECRET = os.getenv("UPSTOX_CLIENT_SECRET")
    UPSTOX_REDIRECT_URI = os.getenv("UPSTOX_REDIRECT_URI", "http://localhost:8888/callback")
    NIFTY_KEY = "NSE_INDEX|Nifty 50"
    VIX_KEY = "NSE_INDEX|India VIX"
    BASE_CAPITAL = int(os.getenv("VG_BASE_CAPITAL", "1000000"))
    MARGIN_SELL_BASE = 125000
    MARGIN_BUY_BASE = 30000
    MAX_CAPITAL_USAGE = 0.80
    DAILY_LOSS_LIMIT = 0.03
    MAX_POSITION_SIZE = 0.25
    MAX_LOSS_PER_TRADE = int(os.getenv("VG_MAX_LOSS_PER_TRADE", "50000"))
    MAX_CAPITAL_PER_TRADE = int(os.getenv("VG_MAX_CAPITAL_PER_TRADE", "300000"))
    MAX_TRADES_PER_DAY = int(os.getenv("VG_MAX_TRADES_PER_DAY", "3"))
    MAX_DRAWDOWN_PCT = float(os.getenv("VG_MAX_DRAWDOWN_PCT", "0.15"))
    MAX_CONTRACTS_PER_INSTRUMENT = 1800
    PRICE_CHANGE_THRESHOLD = 0.10
    IRON_FLY_MIN_WING_WIDTH = 100
    IRON_FLY_MAX_WING_WIDTH = 400
    IRON_FLY_WING_DELTA_TARGET = 0.10
    IRON_FLY_ATM_TOLERANCE = 0.02
    GAMMA_DANGER_DTE = 1
    GEX_STICKY_RATIO = 0.03
    HIGH_VOL_IVP = 75.0
    LOW_VOL_IVP = 25.0
    VOV_CRASH_ZSCORE = 2.5
    VOV_WARNING_ZSCORE = 2.0
    WEIGHT_VOL = 0.40
    WEIGHT_STRUCT = 0.30
    WEIGHT_EDGE = 0.20
    WEIGHT_RISK = 0.10
    FII_STRONG_LONG = 50000
    FII_STRONG_SHORT = -50000
    FII_MODERATE = 20000
    TARGET_PROFIT_PCT = 0.50
    STOP_LOSS_PCT = 1.0
    MAX_SHORT_DELTA = 0.20
    EXIT_DTE = 1
    SLIPPAGE_TOLERANCE = 0.02
    PARTIAL_FILL_TOLERANCE = 0.95
    HEDGE_FILL_TOLERANCE = 0.98
    ORDER_TIMEOUT = 10
    MAX_BID_ASK_SPREAD = 0.05
    POLL_INTERVAL = 0.5
    ANALYSIS_INTERVAL = 1800
    MAX_API_RETRIES = 3
    DASHBOARD_REFRESH_RATE = 1.0
    PRICE_STALENESS_THRESHOLD = 5
    DB_PATH = os.getenv("VG_DB_PATH", "/app/data/volguard.db")
    LOG_DIR = os.getenv("VG_LOG_DIR", "/app/logs")
    LOG_FILE = os.path.join(LOG_DIR, f"volguard_{ENVIRONMENT.lower()}.log")
    LOG_LEVEL = logging.INFO
    MARKET_OPEN = (9, 15)
    MARKET_CLOSE = (15, 30)
    SAFE_ENTRY_START = (9, 0)
    SAFE_EXIT_END = (15, 15)
    MAX_CONSECUTIVE_LOSSES = 3
    COOL_DOWN_PERIOD = 86400
    MAX_SLIPPAGE_EVENTS_PER_DAY = 5
    ANALYTICS_PROCESS_TIMEOUT = 300
    DB_WRITER_QUEUE_MAX_SIZE = 10000
    HEARTBEAT_INTERVAL = 30
    WEBSOCKET_RECONNECT_DELAY = 5
    MAX_ZOMBIE_PROCESSES = 3
    KILL_SWITCH_FILE = os.getenv("VG_KILL_SWITCH_FILE", "/app/data/KILL_SWITCH")
    POSITION_RECONCILE_INTERVAL = 300
    MARGIN_BUFFER = 0.20
    DRY_RUN_SLIPPAGE_MEAN = 0.001
    DRY_RUN_SLIPPAGE_STD = 0.0005
    DRY_RUN_FILL_PROBABILITY = 0.95
    DEFAULT_STRIKE_INTERVAL = 50
    MIN_STRIKE_OI = 1000
    WING_FACTOR_EXTREME_VOL = 1.4
    WING_FACTOR_HIGH_VOL = 1.1
    WING_FACTOR_LOW_VOL = 0.8
    WING_FACTOR_STANDARD = 1.0
    IVP_THRESHOLD_EXTREME = 80.0
    IVP_THRESHOLD_HIGH = 50.0
    IVP_THRESHOLD_LOW = 20.0
    MIN_WING_INTERVAL_MULTIPLIER = 2
    DELTA_SHORT_WEEKLY = 0.20
    DELTA_SHORT_MONTHLY = 0.16
    DELTA_LONG_HEDGE = 0.05
    DELTA_CREDIT_SHORT = 0.30
    DELTA_CREDIT_LONG = 0.10
    TREND_BULLISH_THRESHOLD = 0.0
    PCR_BULLISH_THRESHOLD = 1.0
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    IST = pytz.timezone('Asia/Kolkata')
    MAX_PORTFOLIO_DELTA = float(os.getenv("VG_MAX_DELTA", "500"))
    MAX_PORTFOLIO_VEGA = float(os.getenv("VG_MAX_VEGA", "100000"))
    MAX_POSITION_CORRELATION = 0.85
    TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
    TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
    TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM")
    TWILIO_WHATSAPP_TO = os.getenv("TWILIO_WHATSAPP_TO")
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    EMAIL_TO = os.getenv("EMAIL_TO")
    VG_ENCRYPTION_KEY = os.getenv("VG_ENCRYPTION_KEY")

    @classmethod
    def validate(cls):
        missing = []
        if not cls.DRY_RUN_MODE:
            if not cls.UPSTOX_ACCESS_TOKEN: missing.append("UPSTOX_ACCESS_TOKEN")
        if not cls.TELEGRAM_BOT_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.TELEGRAM_CHAT_ID: missing.append("TELEGRAM_CHAT_ID")
        if missing: raise EnvironmentError(f"Missing: {', '.join(missing)}")

# ----------  LOGGING  ----------
os.makedirs(ProductionConfig.LOG_DIR, exist_ok=True)
file_handler = RotatingFileHandler(ProductionConfig.LOG_FILE, maxBytes=10*1024*1024, backupCount=5)
stream_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(level=ProductionConfig.LOG_LEVEL, format='%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s', handlers=[file_handler, stream_handler])
logger = logging.getLogger("VOLGUARD")
if ProductionConfig.DRY_RUN_MODE:
    logger.warning("="*80)
    logger.warning("🎯 DRY RUN MODE ENABLED - NO REAL TRADES WILL BE EXECUTED")
    logger.warning("="*80)

# ----------  TELEGRAM + WHATSAPP  ----------
class TelegramAlerter:
    def __init__(self):
        self.bot_token = ProductionConfig.TELEGRAM_BOT_TOKEN
        self.chat_id = ProductionConfig.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.queue = queue.Queue()
        self.running = True
        self.worker = threading.Thread(target=self._process_queue, daemon=True, name="Telegram-Worker")
        self.worker.start()

    def _process_queue(self):
        while self.running:
            try:
                msg_data = self.queue.get(timeout=1)
                message, level, retry, use_whatsapp_backup = msg_data
                emoji_map = {"CRITICAL": "🚨", "ERROR": "❌", "WARNING": "⚠️", "INFO": "ℹ️", "SUCCESS": "✅", "TRADE": "💰", "SYSTEM": "⚙️"}
                prefix = emoji_map.get(level, "📢")
                full_msg = f"{prefix} *VOLGUARD 3.2*\n{message}"
                telegram_success = False
                for attempt in range(retry):
                    try:
                        resp = requests.post(f"{self.base_url}/sendMessage", json={"chat_id": self.chat_id, "text": full_msg, "parse_mode": "Markdown"}, timeout=5)
                        if resp.status_code == 200: telegram_success = True; break
                    except: time.sleep(1)
                if not telegram_success and level in ["CRITICAL", "ERROR"] and use_whatsapp_backup:
                    logger.warning("📱 Telegram failed - trying WhatsApp backup")
                    whatsapp.send(message, level)
                self.queue.task_done()
            except queue.Empty: continue
            except Exception as e: logger.error(f"Telegram Worker Crash: {e}")

    def send(self, message: str, level: str = "INFO", retry: int = 3, use_whatsapp_backup: bool = True) -> bool:
        try: self.queue.put((message, level, retry, use_whatsapp_backup)); return True
        except: logger.error("Failed to queue alert"); return False

    def shutdown(self):
        self.running = False
        try: self.worker.join(timeout=2)
        except: pass

telegram = TelegramAlerter()

class WhatsAppAlerter:
    def __init__(self):
        self.account_sid = ProductionConfig.TWILIO_ACCOUNT_SID
        self.auth_token = ProductionConfig.TWILIO_AUTH_TOKEN
        self.from_number = ProductionConfig.TWILIO_WHATSAPP_FROM
        self.to_number = ProductionConfig.TWILIO_WHATSAPP_TO
        self.enabled = all([self.account_sid, self.auth_token, self.from_number, self.to_number])
        if not self.enabled: logger.warning("⚠️ WhatsApp alerts not configured (optional)")

    def send(self, message: str, level: str = "INFO") -> bool:
        if not self.enabled: return False
        try:
            from twilio.rest import Client
            client = Client(self.account_sid, self.auth_token)
            emoji_map = {"CRITICAL": "🚨", "ERROR": "❌", "WARNING": "⚠️", "INFO": "ℹ️", "SUCCESS": "✅"}
            prefix = emoji_map.get(level, "📢")
            full_message = f"{prefix} *VolGuard 3.2*\n\n{message}"
            msg = client.messages.create(from_=self.from_number, body=full_message, to=self.to_number)
            logger.debug(f"WhatsApp sent: {msg.sid}"); return True
        except Exception as e: logger.error(f"WhatsApp send failed: {e}"); return False

whatsapp = WhatsAppAlerter()

# ----------  SECURE STORAGE  ----------
class SecureStorage:
    def __init__(self):
        secret = ProductionConfig.VG_ENCRYPTION_KEY.encode() if ProductionConfig.VG_ENCRYPTION_KEY else b'default-insecure-key-change-me'
        key = base64.urlsafe_b64encode(hashlib.sha256(secret).digest())
        self.cipher = Fernet(key)
        if not ProductionConfig.VG_ENCRYPTION_KEY: logger.warning("⚠️ Using default encryption key - SET VG_ENCRYPTION_KEY in production!")
    def encrypt(self, plaintext: str) -> str:
        if not plaintext: return ""
        return base64.urlsafe_b64encode(self.cipher.encrypt(plaintext.encode())).decode()
    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext: return ""
        try: return self.cipher.decrypt(base64.urlsafe_b64decode(ciphertext.encode())).decode()
        except Exception as e: logger.error(f"Decryption failed: {e}"); return ""

secure_storage = SecureStorage()

# ----------  CONFIG HOT-RELOAD  ----------
class ConfigManager:
    def __init__(self, config_path: str = "/app/config/config.yaml"):
        self.config_path = config_path
        self.last_load_time = 0
        self.reload_interval = 60
        if not os.path.exists(config_path):
            default = """
max_portfolio_delta: 500
max_portfolio_vega: 100000
enable_ai_morning_brief: true
enable_whatsapp_backup: true
enable_position_reconciliation: true
enable_daily_email_report: true
"""
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            with open(config_path, 'w') as f: f.write(default)
    def reload_if_changed(self):
        try:
            if not os.path.exists(self.config_path): return
            mtime = os.path.getmtime(self.config_path)
            if mtime > self.last_load_time:
                logger.info("🔄 Config file changed - reloading...")
                with open(self.config_path) as f: config = yaml.safe_load(f)
                for k, v in config.items():
                    attr = k.upper()
                    if hasattr(ProductionConfig, attr):
                        setattr(ProductionConfig, attr, v)
                        logger.debug(f"Config updated: {attr} = {v}")
                self.last_load_time = mtime
                telegram.send("⚙️ Configuration updated", "SYSTEM")
        except Exception as e: logger.error(f"Config reload failed: {e}")

config_manager = ConfigManager()

# ----------  RATE LIMITER  ----------
class RateLimiter:
    def __init__(self, requests_per_second: int = 8):
        self.rate = requests_per_second
        self.tokens = requests_per_second
        self.max_tokens = requests_per_second
        self.last_update = time.time()
        self.lock = threading.Lock()
    def acquire(self, timeout: float = 5.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
                self.last_update = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True
            time.sleep(0.1)
        logger.warning("⚠️ Rate limit - request timeout")
        return False

api_rate_limiter = RateLimiter()

# ----------  MARKET DATA CACHE  ----------
class MarketDataCache:
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()
    def get(self, key: str, ttl: int) -> Optional[Any]:
        with self.lock:
            if key in self.cache:
                value, timestamp = self.cache[key]
                if time.time() - timestamp < ttl: return value
                else: del self.cache[key]
            return None
    def set(self, key: str, value: Any):
        with self.lock: self.cache[key] = (value, time.time())
    def invalidate(self, key: str = None):
        with self.lock:
            if key:
                if key in self.cache: del self.cache[key]
            else: self.cache.clear()

market_cache = MarketDataCache()

# ----------  DATABASE WRITER  ----------
class DatabaseWriter:
    def __init__(self, db_path: str = ProductionConfig.DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.message_queue = queue.Queue(maxsize=ProductionConfig.DB_WRITER_QUEUE_MAX_SIZE)
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True, name="DB-Writer")
        self.thread.start()
        self._init_schema()
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
        return conn
    def _init_schema(self):
        conn = self._get_connection()
        schema = """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            strategy_type TEXT,
            expiry_date DATE,
            entry_premium REAL,
            max_risk REAL,
            status TEXT,
            legs_json TEXT,
            exit_reason TEXT,
            final_pnl REAL
        );
        CREATE TABLE IF NOT EXISTS positions (
            position_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            instrument_key TEXT,
            strike REAL,
            option_type TEXT,
            side TEXT,
            qty INTEGER,
            entry_price REAL,
            current_price REAL,
            delta REAL,
            status TEXT,
            FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
        );
        CREATE TABLE IF NOT EXISTS risk_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT,
            severity TEXT,
            description TEXT,
            action_taken TEXT
        );
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS order_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            order_id TEXT,
            instrument_key TEXT,
            side TEXT,
            qty INTEGER,
            price REAL,
            status TEXT,
            filled_qty INTEGER,
            avg_price REAL,
            message TEXT
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            paper_id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            instrument_key TEXT,
            side TEXT,
            qty INTEGER,
            entry_price REAL,
            exit_price REAL,
            pnl REAL,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS performance_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE,
            total_trades INTEGER,
            winning_trades INTEGER,
            losing_trades INTEGER,
            total_pnl REAL,
            peak_capital REAL,
            current_capital REAL,
            drawdown_pct REAL,
            sharpe_ratio REAL
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE,
            trades_executed INTEGER DEFAULT 0,
            total_pnl REAL DEFAULT 0,
            largest_win REAL DEFAULT 0,
            largest_loss REAL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_risk_events_timestamp ON risk_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_order_log_timestamp ON order_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_paper_trades_timestamp ON paper_trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date);
        """
        conn.executescript(schema); conn.commit(); conn.close()
    def _worker(self):
        conn = self._get_connection()
        while self.running:
            try:
                msg = self.message_queue.get(timeout=1)
                if msg['type'] == 'execute':
                    conn.execute(msg['sql'], msg['params']); conn.commit()
                elif msg['type'] == 'executescript':
                    conn.executescript(msg['sql']); conn.commit()
                elif msg['type'] == 'shutdown': break
            except queue.Empty: continue
            except Exception as e:
                logger.error(f"DB write error: {e}"); conn.rollback()
        conn.close()
    def execute(self, sql: str, params: tuple = ()):
        try: self.message_queue.put({'type': 'execute', 'sql': sql, 'params': params}, timeout=1)
        except queue.Full: logger.error("DB queue full - dropping write")
    def set_state(self, key: str, value: str):
        self.execute("INSERT OR REPLACE INTO system_state (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)", (key, value))
    def get_state(self, key: str) -> Optional[str]:
        try:
            conn = self._get_connection(); cur = conn.cursor()
            cur.execute("SELECT value FROM system_state WHERE key = ?", (key,))
            row = cur.fetchone(); conn.close(); return row[0] if row else None
        except: return None
    def save_trade(self, trade_id: str, strategy: str, expiry: date, legs: List[Dict], entry_premium: float, max_risk: float):
        self.execute("INSERT INTO trades (trade_id, strategy_type, expiry_date, entry_premium, max_risk, status, legs_json) VALUES (?, ?, ?, ?, ?, 'OPEN', ?)",
                     (trade_id, strategy, expiry, entry_premium, max_risk, json.dumps(legs)))
    def update_trade_exit(self, trade_id: str, exit_reason: str, final_pnl: float):
        self.execute("UPDATE trades SET status='CLOSED', exit_reason=?, final_pnl=? WHERE trade_id=?", (exit_reason, final_pnl, trade_id))
    def log_risk_event(self, event_type: str, severity: str, desc: str, action: str):
        self.execute("INSERT INTO risk_events (event_type, severity, description, action_taken) VALUES (?, ?, ?, ?)", (event_type, severity, desc, action))
    def log_order(self, order_id: str, instrument_key: str, side: str, qty: int, price: float, status: str, filled_qty: int = 0, avg_price: float = 0.0, message: str = ""):
        self.execute("INSERT INTO order_log (order_id, instrument_key, side, qty, price, status, filled_qty, avg_price, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                     (order_id, instrument_key, side, qty, price, status, filled_qty, avg_price, message))
    def get_daily_stats(self, target_date: date = None) -> Optional[Dict]:
        if not target_date: target_date = date.today()
        try:
            conn = self._get_connection(); cur = conn.cursor()
            cur.execute("SELECT * FROM daily_stats WHERE date = ?", (target_date,))
            row = cur.fetchone(); conn.close()
            if row: return dict(row)
            return None
        except: return None
    def update_daily_stats(self, trades: int = 0, pnl: float = 0, largest_win: float = 0, largest_loss: float = 0):
        today = date.today()
        self.execute("INSERT INTO daily_stats (date, trades_executed, total_pnl, largest_win, largest_loss) VALUES (?, ?, ?, ?, ?) "
                     "ON CONFLICT(date) DO UPDATE SET trades_executed=trades_executed+?, total_pnl=total_pnl+?, largest_win=MAX(largest_win,?), largest_loss=MIN(largest_loss,?)",
                     (today, trades, pnl, largest_win, largest_loss, trades, pnl, largest_win, largest_loss))
    def export_trade_journal(self, output_path: str):
        try:
            os.makedirs(output_path, exist_ok=True)
            conn = self._get_connection()
            pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC", conn).to_csv(os.path.join(output_path, "trades.csv"), index=False)
            pd.read_sql_query("SELECT * FROM risk_events ORDER BY timestamp DESC", conn).to_csv(os.path.join(output_path, "risk_events.csv"), index=False)
            if ProductionConfig.DRY_RUN_MODE:
                pd.read_sql_query("SELECT * FROM paper_trades ORDER BY timestamp DESC", conn).to_csv(os.path.join(output_path, "paper_trades.csv"), index=False)
            conn.close(); return True
        except Exception as e: logger.error(f"Export failed: {e}"); return False
    def shutdown(self):
        self.running = False
        try: self.message_queue.put({'type': 'shutdown'}, timeout=2)
        except: pass
        self.thread.join(timeout=10)

db_writer = DatabaseWriter()

# ----------  CIRCUIT BREAKER  ----------
class CircuitBreaker:
    def __init__(self, db_writer: DatabaseWriter):
        self.db_writer = db_writer
        self.consecutive_losses = int(db_writer.get_state("consecutive_losses") or 0)
        self.breaker_triggered = False
        self.breaker_until = None
        self.daily_slippage_events = 0
        self.last_reset_date = date.today()
        self.peak_capital = float(db_writer.get_state("peak_capital") or ProductionConfig.BASE_CAPITAL)
        self.current_capital = ProductionConfig.BASE_CAPITAL

    def update_capital(self, new_capital: float):
        self.current_capital = new_capital
        if new_capital > self.peak_capital:
            self.peak_capital = new_capital
            self.db_writer.set_state("peak_capital", str(self.peak_capital))
        drawdown = (self.peak_capital - new_capital) / self.peak_capital
        if drawdown >= ProductionConfig.MAX_DRAWDOWN_PCT:
            self.trigger_breaker("MAX_DRAWDOWN", f"Drawdown: {drawdown*100:.1f}%")
            return False
        return True

    def check_daily_loss_limit(self, current_pnl: float) -> bool:
        loss_pct = abs(current_pnl) / self.peak_capital
        if current_pnl < 0 and loss_pct >= ProductionConfig.DAILY_LOSS_LIMIT:
            self.trigger_breaker("DAILY_LOSS_LIMIT", f"Loss: ₹{current_pnl:,.2f} ({loss_pct*100:.1f}% of peak ₹{self.peak_capital:,.0f})")
            return False
        return True

    def record_slippage_event(self, slippage_pct: float) -> bool:
        self.daily_slippage_events += 1
        if self.daily_slippage_events >= ProductionConfig.MAX_SLIPPAGE_EVENTS_PER_DAY:
            self.trigger_breaker("EXCESSIVE_SLIPPAGE", f"{self.daily_slippage_events} events today")
            return False
        return True

    def record_trade_result(self, pnl: float) -> bool:
        if pnl < 0:
            self.consecutive_losses += 1
            self.db_writer.set_state("consecutive_losses", str(self.consecutive_losses))
            if self.consecutive_losses >= ProductionConfig.MAX_CONSECUTIVE_LOSSES:
                self.trigger_breaker("CONSECUTIVE_LOSSES", f"{self.consecutive_losses} losses")
                return False
        else:
            if self.consecutive_losses > 0:
                logger.info(f"Winning trade after {self.consecutive_losses} losses - resetting counter")
                self.consecutive_losses = 0
                self.db_writer.set_state("consecutive_losses", str(self.consecutive_losses))
        return True

    def trigger_breaker(self, reason: str, details: str):
        self.breaker_triggered = True
        self.breaker_until = datetime.now() + timedelta(seconds=ProductionConfig.COOL_DOWN_PERIOD)
        telegram.send(f"🔴 *CIRCUIT BREAKER*\n{reason}: {details}\nCooldown until: {self.breaker_until.strftime('%H:%M:%S')}", "CRITICAL")
        self.db_writer.log_risk_event("CIRCUIT_BREAKER", "CRITICAL", reason, details)
        logger.critical(f"CIRCUIT BREAKER: {reason} - {details}")
        circuit_breaker_active.set(1)

    def is_active(self) -> bool:
        if os.path.exists(ProductionConfig.KILL_SWITCH_FILE):
            logger.critical(f"KILL SWITCH DETECTED: {ProductionConfig.KILL_SWITCH_FILE}")
            self.trigger_breaker("KILL_SWITCH", "Manual emergency stop")
            return True
        if self.breaker_triggered and self.breaker_until:
            if datetime.now() > self.breaker_until:
                logger.info("Circuit breaker cooldown expired - resetting")
                self.breaker_triggered = False; self.breaker_until = None
                telegram.send("Circuit breaker cooldown expired - system ready", "SYSTEM")
                circuit_breaker_active.set(0); return False
        return self.breaker_triggered

circuit_breaker = CircuitBreaker(db_writer)

# ----------  HEARTBEAT  ----------
class Heartbeat:
    def __init__(self):
        self.file_path = os.path.join(os.path.dirname(ProductionConfig.DB_PATH), "heartbeat")
        self.running = True
    def beat(self):
        try:
            with open(self.file_path, 'w') as f: f.write(str(time.time()))
        except: pass
    def stop(self):
        self.running = False
        try:
            if os.path.exists(self.file_path): os.remove(self.file_path)
        except: pass

heartbeat = Heartbeat()

# ----------  PAPER TRADING  ----------
class PaperTradingEngine:
    def __init__(self):
        self.paper_positions = {}
        self.paper_orders = {}
        self.order_counter = 0
        self.lock = threading.Lock()
    def place_order(self, instrument_key: str, qty: int, side: str, order_type: str, price: float) -> Optional[str]:
        with self.lock:
            self.order_counter += 1
            order_id = f"PAPER_{int(time.time())}_{self.order_counter}"
            if np.random.random() > ProductionConfig.DRY_RUN_FILL_PROBABILITY:
                self.paper_orders[order_id] = {'status': 'rejected', 'filled_qty': 0, 'avg_price': 0}
                return order_id
            slippage = np.random.normal(ProductionConfig.DRY_RUN_SLIPPAGE_MEAN, ProductionConfig.DRY_RUN_SLIPPAGE_STD)
            fill_price = price * (1 + slippage) if side == 'BUY' else price * (1 - slippage)
            fill_price = round(fill_price, 1)
            self.paper_orders[order_id] = {'status': 'complete', 'filled_qty': qty, 'avg_price': fill_price, 'instrument_key': instrument_key, 'side': side}
            pos_key = f"{instrument_key}_{side}"
            if pos_key not in self.paper_positions: self.paper_positions[pos_key] = {'qty': 0, 'avg_price': 0, 'instrument_key': instrument_key, 'side': side}
            pos = self.paper_positions[pos_key]; pos['qty'] += qty; pos['avg_price'] = fill_price
            logger.info(f"📄 PAPER ORDER FILLED: {side} {qty}x {instrument_key} @ {fill_price} (slippage: {slippage*100:.2f}%)")
            return order_id
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        with self.lock: return self.paper_orders.get(order_id)
    def cancel_order(self, order_id: str) -> bool:
        with self.lock:
            if order_id in self.paper_orders: self.paper_orders[order_id]['status'] = 'cancelled'; return True
            return False
    def get_positions(self) -> List[Dict]:
        with self.lock: return list(self.paper_positions.values())
    def clear_position(self, instrument_key: str, side: str):
        with self.lock:
            pos_key = f"{instrument_key}_{side}"
            if pos_key in self.paper_positions: del self.paper_positions[pos_key]

paper_engine = PaperTradingEngine()

# ----------  TELEGRAM TOKEN MANAGER  ----------
class TelegramTokenManager:
    def __init__(self, telegram_alerter: TelegramAlerter):
        self.telegram = telegram_alerter
        self.bot_token = ProductionConfig.TELEGRAM_BOT_TOKEN
        self.chat_id = ProductionConfig.TELEGRAM_CHAT_ID
        self.auth_code = None
        self.token_received = threading.Event()
        self.webhook_server = None
    def generate_auth_url(self) -> str:
        base_url = "https://api.upstox.com/v2/login/authorization/dialog"
        params = {'client_id': ProductionConfig.UPSTOX_CLIENT_ID, 'redirect_uri': ProductionConfig.UPSTOX_REDIRECT_URI, 'response_type': 'code', 'state': f"volguard_{int(time.time())}"}
        from urllib.parse import urlencode
        return f"{base_url}?{urlencode(params)}"
    def start_callback_server(self):
        manager_self = self
        class CallbackHandler(BaseHTTPRequestHandler):
            def log_message(self, format, *args): pass
            def do_GET(self):
                parsed = urlparse(self.path); params = parse_qs(parsed.query)
                if 'code' in params:
                    manager_self.auth_code = params['code'][0]
                    self.send_response(200); self.send_header('Content-type', 'text/html'); self.end_headers()
                    self.wfile.write(b"""
                        <html><body style="font-family: Arial; text-align: center; padding: 50px;">
                        <h1 style="color: green;">✅ Authentication Successful!</h1>
                        <p>You can close this window now.</p>
                        <p>VolGuard will start trading shortly.</p>
                        </body></html>
                    """)
                    logger.info(f"✅ OAuth code received: {manager_self.auth_code[:20]}...")
                    manager_self.token_received.set()
                    threading.Thread(target=manager_self.webhook_server.shutdown, daemon=True).start()
                else:
                    self.send_response(400); self.send_header('Content-type', 'text/html'); self.end_headers()
                    self.wfile.write(b"<h1>Error: No authorization code received</h1>")
        try:
            self.webhook_server = HTTPServer(('0.0.0.0', 8888), CallbackHandler)
            logger.info("🌐 OAuth callback server started on port 8888")
            self.webhook_server.serve_forever()
        except Exception as e: logger.error(f"Failed to start callback server: {e}")
    def request_token_via_telegram(self) -> bool:
        try:
            server_thread = threading.Thread(target=self.start_callback_server, daemon=True, name="OAuth-Callback-Server"); server_thread.start()
            time.sleep(1)
            auth_url = self.generate_auth_url()
            message = (
                "🔐 *DAILY AUTHENTICATION REQUIRED*\n\n"
                "Your Upstox access token has expired.\n"
                "Please click the button below to authorize VolGuard.\n\n"
                "⏰ *You have 10 minutes to complete this.*\n\n"
                "After clicking:\n1️⃣ Login to Upstox\n2️⃣ Click 'Authorize'\n3️⃣ Wait for confirmation\n\n"
                "_Trading will start automatically after authorization._"
            )
            payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "Markdown", "reply_markup": {"inline_keyboard": [[{"text": "🔐 Authorize Upstox", "url": auth_url}]]}}
            resp = requests.post(f"https://api.telegram.org/bot{self.bot_token}/sendMessage", json=payload, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Failed to send Telegram button: {resp.text}")
                self.telegram.send(f"🔐 Please authorize:\n{auth_url}", "CRITICAL")
            logger.info("⏳ Waiting for user authentication...")
            self.telegram.send("⏳ Waiting for your authorization...", "INFO")
            if self.token_received.wait(timeout=600):
                success = self._exchange_code_for_token()
                if success:
                    logger.info("✅ Access token obtained successfully")
                    self.telegram.send("✅ Authentication successful! Starting trading.", "SUCCESS")
                    return True
                else:
                    logger.error("❌ Token exchange failed")
                    self.telegram.send("❌ Token exchange failed. Please retry.", "ERROR")
                    return False
            else:
                logger.error("❌ Authentication timeout (10 minutes)")
                self.telegram.send("⏱ Authentication timeout. Please run manually:\n`python volguard.py --authenticate`", "ERROR")
                return False
        except Exception as e:
            logger.error(f"Token request failed: {e}")
            self.telegram.send(f"❌ Authentication error: {e}", "ERROR")
            return False
    def _exchange_code_for_token(self) -> bool:
        try:
            url = "https://api.upstox.com/v2/login/authorization/token"
            payload = {'code': self.auth_code, 'client_id': ProductionConfig.UPSTOX_CLIENT_ID, 'client_secret': ProductionConfig.UPSTOX_CLIENT_SECRET,
                       'redirect_uri': ProductionConfig.UPSTOX_REDIRECT_URI, 'grant_type': 'authorization_code'}
            headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
            resp = requests.post(url, data=payload, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                access_token = data.get('access_token')
                if access_token:
                    encrypted = secure_storage.encrypt(access_token)
                    db_writer.set_state('upstox_access_token_encrypted', encrypted)
                    db_writer.set_state('token_obtained_at', datetime.now().isoformat())
                    ProductionConfig.UPSTOX_ACCESS_TOKEN = access_token
                    logger.info(f"✅ Access token stored (encrypted)")
                    return True
            logger.error(f"Token exchange failed: {resp.status_code} - {resp.text}")
            return False
        except Exception as e:
            logger.error(f"Token exchange error: {e}")
            return False
    def check_token_validity(self) -> bool:
        try:
            token_time_str = db_writer.get_state('token_obtained_at')
            if not token_time_str: return False
            token_time = datetime.fromisoformat(token_time_str)
            now = datetime.now()
            if token_time.date() < now.date(): return False
            if now.time() >= dtime(3, 30) and token_time.time() < dtime(3, 30): return False
            return True
        except: return False
    def ensure_valid_token(self) -> bool:
        if self.check_token_validity():
            logger.info("✅ Existing token is valid")
            return True
        logger.warning("⚠️ Token expired or missing - requesting new token")
        return self.request_token_via_telegram()

token_manager = TelegramTokenManager(telegram)

# ----------  POSITION MONITOR  ----------
class PositionMonitor:
    def __init__(self, api_client: upstox_client.ApiClient, db_writer: DatabaseWriter):
        self.api_client = api_client
        self.db_writer = db_writer
        self.last_reconcile = 0
        self.reconcile_interval = 300
    def get_broker_positions(self) -> List[Dict]:
        try:
            portfolio_api = PortfolioApi(self.api_client)
            resp = portfolio_api.get_positions(api_version="2.0")
            if resp.status != 'success' or not resp.data: return []
            return [{
                'instrument_key': p.instrument_token,
                'symbol': p.trading_symbol,
                'qty': int(p.quantity) if hasattr(p, 'quantity') else 0,
                'avg_price': float(p.average_price) if hasattr(p, 'average_price') else 0.0,
                'ltp': float(p.last_price) if hasattr(p, 'last_price') else 0.0,
                'pnl': float(p.pnl) if hasattr(p, 'pnl') else 0.0
            } for p in resp.data if int(getattr(p, 'quantity', 0)) != 0]
        except: return []
    def get_system_positions(self) -> List[Dict]:
        try:
            conn = sqlite3.connect(self.db_writer.db_path, check_same_thread=False, timeout=10)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT trade_id, legs_json FROM trades WHERE status = 'OPEN'")
            rows = cur.fetchall(); conn.close()
            positions = []
            for trade_id, legs_json in rows:
                legs = json.loads(legs_json)
                for leg in legs:
                    positions.append({
                        'trade_id': trade_id,
                        'instrument_key': leg['key'],
                        'qty': leg['filled_qty'] if leg['side'] == 'BUY' else -leg['filled_qty'],
                        'entry_price': leg['entry_price']
                    })
            return positions
        except: return []
    def reconcile(self) -> Dict[str, Any]:
        if time.time() - self.last_reconcile < self.reconcile_interval: return {}
        self.last_reconcile = time.time()
        logger.info("🔍 Running position reconciliation...")
        broker_pos = self.get_broker_positions()
        system_pos = self.get_system_positions()
        broker_dict = {p['instrument_key']: p for p in broker_pos}
        system_dict = {}
        for p in system_pos:
            key = p['instrument_key']
            if key in system_dict: system_dict[key]['qty'] += p['qty']
            else: system_dict[key] = {'qty': p['qty'], 'trade_id': p['trade_id']}
        orphaned = []
        ghost = []
        mismatched = []
        for key, b_data in broker_dict.items():
            if key not in system_dict: orphaned.append({'instrument': b_data['symbol'], 'broker_qty': b_data['qty'], 'system_qty': 0, 'pnl': b_data['pnl']})
            elif b_data['qty'] != system_dict[key]['qty']: mismatched.append({'instrument': b_data['symbol'], 'broker_qty': b_data['qty'], 'system_qty': system_dict[key]['qty'], 'diff': b_data['qty'] - system_dict[key]['qty'], 'trade_id': system_dict[key]['trade_id']})
        for key, s_data in system_dict.items():
            if key not in broker_dict: ghost.append({'instrument_key': key, 'system_qty': s_data['qty'], 'broker_qty': 0, 'trade_id': s_data['trade_id']})
        report = {'timestamp': datetime.now().isoformat(), 'broker_positions': len(broker_pos), 'system_positions': len(system_pos),
                  'orphaned': orphaned, 'ghost': ghost, 'mismatched': mismatched, 'is_clean': len(orphaned) == 0 and len(ghost) == 0 and len(mismatched) == 0}
        if report['is_clean']: logger.info(f"✅ Reconciliation clean: {len(broker_pos)} positions matched")
        else:
            logger.warning(f"⚠️ Reconciliation discrepancies: Orphaned={len(orphaned)}, Ghost={len(ghost)}, Mismatched={len(mismatched)}")
            alert_msg = "⚠️ *POSITION RECONCILIATION ALERT*\n\n"
            if orphaned: alert_msg += f"🔴 *Orphaned ({len(orphaned)})*\n" + "\n".join([f"• {p['instrument']}: {p['broker_qty']} qty (P&L: ₹{p['pnl']:.0f})" for p in orphaned[:3]]) + "\n\n"
            if ghost: alert_msg += f"👻 *Ghost ({len(ghost)})*\n" + "\n".join([f"• Trade {p['trade_id']}: {p['system_qty']} qty expected" for p in ghost[:3]]) + "\n\n"
            if mismatched: alert_msg += f"⚖️ *Qty Mismatches ({len(mismatched)})*\n" + "\n".join([f"• {p['instrument']}: Broker={p['broker_qty']} vs System={p['system_qty']}" for p in mismatched[:3]]) + "\n\n"
            alert_msg += "_Check dashboard for full report_"
            telegram.send(alert_msg, "WARNING")
            self.db_writer.log_risk_event("POSITION_RECONCILIATION", "WARNING", json.dumps(report), "Manual review required")
        return report

# ----------  WEBSOCKET GREEKS MONITOR  ----------
class GreeksMonitor:
    def __init__(self, api_client: upstox_client.ApiClient, legs: List[Dict]):
        self.api_client = api_client
        self.legs = legs
        self.cached_greeks = {}
        self.keys = [leg['key'] for leg in legs]
        self.streamer = None
        self.connected = False
        self._start_stream()

    def _start_stream(self):
        try:
            self.streamer = MarketDataStreamerV3(self.api_client, instrument_keys=self.keys, mode="option_greeks")
            self.streamer.on("open", self._on_open)
            self.streamer.on("close", self._on_close)
            self.streamer.on("message", self._on_message)
            self.streamer.on("error", self._on_error)
            self.streamer.auto_reconnect(True, 3, 5)
            logger.info(f"Connecting to Greeks WebSocket for {len(self.keys)} legs...")
            connect_thread = threading.Thread(target=self.streamer.connect, daemon=True)
            connect_thread.start()
            connect_thread.join(timeout=10)
            if not self.connected:
                logger.error("Greeks WebSocket connection timeout (10s)")
                raise TimeoutError("Greeks stream failed to connect")
        except Exception as e:
            logger.error(f"Failed to start Greeks Streamer: {e}")
            logger.warning("⚠️  FALLBACK: Using static Greeks estimation")

    def _on_open(self):
        self.connected = True
        logger.info(f"✅ Greeks WebSocket Connected ({len(self.keys)} instruments)")

    def _on_close(self):
        self.connected = False
        logger.warning("⚠️  Greeks WebSocket Closed")

    def _on_message(self, message):
        try:
            for key, data in message.items():
                if 'greeks' in data:
                    self.cached_greeks[key] = data['greeks']
                    logger.debug(f"Greeks updated: {key} -> Δ={data['greeks'].get('delta', 0):.3f}")
        except Exception as e:
            logger.error(f"Greeks message parse error: {e}")

    def _on_error(self, error):
        logger.error(f"Greeks Stream Error: {error}")

    def calculate_portfolio_greeks(self, prices: Dict) -> Dict[str, float]:
        total_delta = 0.0; total_gamma = 0.0; total_theta = 0.0; total_vega = 0.0
        for leg in self.legs:
            key = leg['key']; qty = leg['filled_qty']; multiplier = 1 if leg['side'] == 'BUY' else -1
            greeks = self.cached_greeks.get(key)
            if greeks:
                d = float(greeks.get('delta', 0.0)); g = float(greeks.get('gamma', 0.0)); t = float(greeks.get('theta', 0.0)); v = float(greeks.get('vega', 0.0))
            else:
                logger.warning(f"No Greeks data for {key} - using fallback")
                d = 0.5 if leg.get('type') == 'CE' else -0.5; g = 0.01; t = -leg.get('strike', 23500) / 365; v = leg.get('strike', 23500) * 0.02
            total_delta += d * qty * multiplier; total_gamma += g * qty * multiplier; total_theta += t * qty * multiplier; total_vega += v * qty * multiplier
        update_greeks(total_delta, total_theta, total_gamma, total_vega)
        return {'delta': total_delta, 'gamma': total_gamma, 'theta': total_theta, 'vega': total_vega, 'timestamp': time.time()}

    def check_greek_limits(self, greeks: Dict[str, float]) -> List[str]:
        warnings = []
        if abs(greeks['delta']) > ProductionConfig.MAX_PORTFOLIO_DELTA: warnings.append(f"⚠️  Delta: {greeks['delta']:.0f} (limit: ±{ProductionConfig.MAX_PORTFOLIO_DELTA})")
        if abs(greeks['vega']) > ProductionConfig.MAX_PORTFOLIO_VEGA: warnings.append(f"⚠️  Vega: ₹{greeks['vega']:,.0f} (limit: ₹{ProductionConfig.MAX_PORTFOLIO_VEGA:,.0f})")
        if greeks['gamma'] < -1000: warnings.append(f"⚠️  Gamma: {greeks['gamma']:.0f} (high convexity risk)")
        return warnings

    def stop(self):
        if self.streamer:
            try: self.streamer.disconnect(); logger.info("Greeks WebSocket disconnected")
            except Exception as e: logger.error(f"Error stopping Greeks stream: {e}")

# ----------  PERFORMANCE REPORTER  ----------
class PerformanceReporter:
    def __init__(self, db_writer: DatabaseWriter):
        self.db_writer = db_writer
    def generate_daily_report(self, report_date: date = None) -> str:
        if not report_date: report_date = date.today() - timedelta(days=1)
        try:
            conn = sqlite3.connect(self.db_writer.db_path, check_same_thread=False, timeout=10)
            conn.row_factory = sqlite3.Row; cur = conn.cursor()
            cur.execute("SELECT trades_executed, total_pnl, largest_win, largest_loss FROM daily_stats WHERE date = ?", (report_date,))
            row = cur.fetchone()
            if not row: return None
            trades, pnl, win, loss = row
            cur.execute("SELECT trade_id, strategy_type, entry_premium, final_pnl, exit_reason, timestamp FROM trades WHERE DATE(timestamp) = ? AND status = 'CLOSED' ORDER BY timestamp", (report_date,))
            trades_detail = cur.fetchall()
            winners = [t[3] for t in trades_detail if t[3] > 0]; losers = [t[3] for t in trades_detail if t[3] < 0]
            win_rate = len(winners) / len(trades_detail) * 100 if trades_detail else 0
            avg_win = sum(winners) / len(winners) if winners else 0
            avg_loss = sum(losers) / len(losers) if losers else 0
            profit_factor = abs(sum(winners) / sum(losers)) if losers else float('inf')
            report = f"""
╔════════════════════════════════════════════════════════════╗
║              VOLGUARD DAILY PERFORMANCE REPORT             ║
║                {report_date.strftime('%A, %d %B %Y')}                ║
╚════════════════════════════════════════════════════════════╝

📊 SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total Trades:          {trades}
Net P&L:               ₹{pnl:,.0f}
Largest Win:           ₹{win:,.0f}
Largest Loss:          ₹{loss:,.0f}

📈 METRICS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Win Rate:              {win_rate:.1f}%
Average Winner:        ₹{avg_win:,.0f}
Average Loser:         ₹{avg_loss:,.0f}
Profit Factor:         {profit_factor:.2f}
Return on Capital:     {(pnl / ProductionConfig.BASE_CAPITAL * 100):.2f}%

💼 TRADE BREAKDOWN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
            for i, trade in enumerate(trades_detail, 1):
                trade_id, strategy, premium, final_pnl, exit_reason, timestamp = trade
                result_emoji = "✅" if final_pnl > 0 else "❌"
                report += f"{i}. {result_emoji} {strategy} | Premium: ₹{premium:,.0f} | P&L: ₹{final_pnl:,.0f} | Exit: {exit_reason}\n"
            report += f"\n{'═'*60}\nReport generated at {datetime.now().strftime('%I:%M %p IST')}\n"
            conn.close(); return report
        except Exception as e: logger.error(f"Report generation failed: {e}"); return None
    def send_email_report(self, report: str, subject: str) -> bool:
        if not (ProductionConfig.SMTP_USER and ProductionConfig.SMTP_PASSWORD and ProductionConfig.EMAIL_TO): return False
        try:
            msg = MIMEMultipart()
            msg['From'] = ProductionConfig.SMTP_USER
            msg['To'] = ProductionConfig.EMAIL_TO
            msg['Subject'] = subject
            msg.attach(MIMEText(report, 'plain'))
            server = smtplib.SMTP(ProductionConfig.SMTP_SERVER, ProductionConfig.SMTP_PORT)
            server.starttls(); server.login(ProductionConfig.SMTP_USER, ProductionConfig.SMTP_PASSWORD)
            server.send_message(msg); server.quit(); return True
        except Exception as e: logger.error(f"Email send failed: {e}"); return False
    def send_daily_report(self, report_date: date = None):
        report = self.generate_daily_report(report_date)
        if not report: return
        self.send_email_report(report, f"VolGuard Daily Report - {report_date.strftime('%d %b %Y')}")
        lines = report.split('\n')
        summary = '\n'.join(lines[5:18])
        telegram.send(f"```\n{summary}\n```", "INFO")

performance_reporter = PerformanceReporter(db_writer)

# ----------  DATA CLASSES  ----------
@dataclass
class TimeMetrics: current_date: date; weekly_exp: date; monthly_exp: date; next_weekly_exp: date; dte_weekly: int; dte_monthly: int; is_gamma_week: bool; is_gamma_month: bool; days_to_next_weekly: int
@dataclass
class VolMetrics: spot: float; vix: float; rv7: float; rv28: float; rv90: float; garch7: float; garch28: float; park7: float; park28: float; vov: float; vov_zscore: float; ivp_30d: float; ivp_90d: float; ivp_1yr: float; ma20: float; atr14: float; trend_strength: float; vol_regime: str; is_fallback: bool
@dataclass
class StructMetrics: net_gex: float; gex_ratio: float; total_oi_value: float; gex_regime: str; pcr: float; max_pain: float; skew_25d: float; oi_regime: str; lot_size: int
@dataclass
class EdgeMetrics: iv_weekly: float; vrp_rv_weekly: float; vrp_garch_weekly: float; vrp_park_weekly: float; iv_monthly: float; vrp_rv_monthly: float; vrp_garch_monthly: float; vrp_park_monthly: float; term_spread: float; term_regime: str; primary_edge: str
@dataclass
class ParticipantData: fut_long: float; fut_short: float; fut_net: float; call_long: float; call_short: float; call_net: float; put_long: float; put_short: float; put_net: float; stock_net: float
@dataclass
class ExternalMetrics: fii: Optional[ParticipantData]; dii: Optional[ParticipantData]; pro: Optional[ParticipantData]; client: Optional[ParticipantData]; fii_net_change: float; flow_regime: str; fast_vol: bool; data_date: str; event_risk: str = "LOW"
@dataclass
class RegimeScore: vol_score: float; struct_score: float; edge_score: float; risk_score: float; composite: float; confidence: str
@dataclass
class TradingMandate: expiry_type: str; expiry_date: date; dte: int; regime_name: str; strategy_type: str; allocation_pct: float; max_lots: int; risk_per_lot: float; score: RegimeScore; rationale: List[str]; warnings: List[str]; suggested_structure: str

# ----------  ANALYTICS ENGINE  ----------
class AnalyticsEngine:
    def __init__(self, result_queue): self.result_queue = result_queue
    def run(self, config: Dict):
        try:
            api_client = upstox_client.ApiClient(); api_client.configuration.access_token = config['access_token']
            history_api = HistoryV3Api(api_client); options_api = OptionsApi(api_client)
            to_date = date.today().strftime("%Y-%m-%d"); from_date = (date.today() - timedelta(days=400)).strftime("%Y-%m-%d")
            nifty_response = history_api.get_historical_candle_data1(ProductionConfig.NIFTY_KEY, "days", "1", to_date, from_date)
            vix_response = history_api.get_historical_candle_data1(ProductionConfig.VIX_KEY, "days", "1", to_date, from_date)
            nifty_hist = self._parse_candle_response(nifty_response); vix_hist = self._parse_candle_response(vix_response)
            market_api = upstox_client.MarketQuoteV3Api(api_client)
            live_prices = market_api.get_ltp(instrument_key=f"{ProductionConfig.NIFTY_KEY},{ProductionConfig.VIX_KEY}")
            weekly, monthly, next_weekly, lot_size = self._get_expiries(options_api)
            weekly_chain = self._get_option_chain(options_api, weekly) if weekly else pd.DataFrame()
            monthly_chain = self._get_option_chain(options_api, monthly) if monthly else pd.DataFrame()
            participant_data, participant_yest, fii_net_change, data_date = self._fetch_participant_data()
            time_metrics = self.get_time_metrics(weekly, monthly, next_weekly)
            vol_metrics = self.get_vol_metrics(nifty_hist, vix_hist, live_prices)
            struct_metrics_weekly = self.get_struct_metrics(weekly_chain, vol_metrics.spot, lot_size)
            struct_metrics_monthly = self.get_struct_metrics(monthly_chain, vol_metrics.spot, lot_size)
            edge_metrics = self.get_edge_metrics(weekly_chain, monthly_chain, vol_metrics.spot, vol_metrics)
            external_metrics = self.get_external_metrics(nifty_hist, participant_data, participant_yest, fii_net_change, data_date)
            iv_rank_weekly.set(vol_metrics.ivp_1yr); iv_rank_monthly.set(vol_metrics.ivp_1yr); vov_zscore.set(vol_metrics.vov_zscore)
            result = {'timestamp': datetime.now(), 'time_metrics': time_metrics, 'vol_metrics': vol_metrics, 'weekly_chain': weekly_chain, 'monthly_chain': monthly_chain,
                      'lot_size': lot_size, 'participant_data': participant_data, 'participant_yest': participant_yest, 'fii_net_change': fii_net_change,
                      'data_date': data_date, 'external_metrics': external_metrics, 'edge_metrics': edge_metrics,
                      'struct_metrics_weekly': struct_metrics_weekly, 'struct_metrics_monthly': struct_metrics_monthly}
            self.result_queue.put(('success', result))
        except Exception as e:
            logger.error(f"Analytics process error: {e}"); traceback.print_exc(); self.result_queue.put(('error', str(e)))
    def _parse_candle_response(self, response):
        if response.status != 'success': return pd.DataFrame()
        candles = response.data.candles if hasattr(response.data, 'candles') else []
        if not candles: return pd.DataFrame()
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
        df['timestamp'] = pd.to_datetime(df['timestamp']); df.set_index('timestamp', inplace=True); return df.astype(float).sort_index()
    def _get_expiries(self, options_api: OptionsApi) -> Tuple[Optional[date], Optional[date], Optional[date], int]:
        try:
            response = options_api.get_option_contracts(instrument_key=ProductionConfig.NIFTY_KEY)
            if response.status != 'success': return None, None, None, 0
            data = response.data; lot_size = next((int(c.lot_size) for c in data if hasattr(c, 'lot_size')), 0)
            expiry_dates = sorted(list(set([
                (c.expiry.date() if hasattr(c.expiry, 'date') else datetime.strptime(str(c.expiry).split('T')[0], "%Y-%m-%d").date())
                for c in data if hasattr(c, 'expiry') and c.expiry
            ])))
            valid_dates = [d for d in expiry_dates if d >= date.today()]
            if not valid_dates: return None, None, None, lot_size
            weekly = valid_dates[0]; next_weekly = valid_dates[1] if len(valid_dates) > 1 else valid_dates[0]
            current_month = date.today().month; current_year = date.today().year
            monthly_candidates = [d for d in valid_dates if d.month == current_month and d.year == current_year]
            if not monthly_candidates or monthly_candidates[-1] < date.today():
                next_month = current_month + 1 if current_month < 12 else 1; next_year = current_year if current_month < 12 else current_year + 1
                monthly_candidates = [d for d in valid_dates if d.month == next_month and d.year == next_year]
            monthly = monthly_candidates[-1] if monthly_candidates else valid_dates[-1]
            return weekly, monthly, next_weekly, lot_size
        except Exception as e: logger.error(f"Expiries fetch error: {e}"); return None, None, None, 0
    def _get_option_chain(self, options_api: OptionsApi, expiry_date: date) -> pd.DataFrame:
        try:
            response = options_api.get_put_call_option_chain(instrument_key=ProductionConfig.NIFTY_KEY, expiry_date=expiry_date.strftime("%Y-%m-%d"))
            if response.status != 'success': return pd.DataFrame()
            return pd.DataFrame([{
                'strike': x.strike_price, 'ce_iv': x.call_options.option_greeks.iv, 'pe_iv': x.put_options.option_greeks.iv,
                'ce_delta': x.call_options.option_greeks.delta, 'pe_delta': x.put_options.option_greeks.delta,
                'ce_gamma': x.call_options.option_greeks.gamma, 'pe_gamma': x.put_options.option_greeks.gamma,
                'ce_oi': x.call_options.market_data.oi, 'pe_oi': x.put_options.market_data.oi,
                'ce_ltp': x.call_options.market_data.ltp, 'pe_ltp': x.put_options.market_data.ltp,
                'ce_bid': getattr(x.call_options.market_data, 'bid_price', 0), 'ce_ask': getattr(x.call_options.market_data, 'ask_price', 0),
                'pe_bid': getattr(x.put_options.market_data, 'bid_price', 0), 'pe_ask': getattr(x.put_options.market_data, 'ask_price', 0),
                'ce_key': x.call_options.instrument_key, 'pe_key': x.put_options.instrument_key
            } for x in response.data])
        except Exception as e: logger.error(f"Option chain fetch error: {e}"); return pd.DataFrame()
    def _fetch_participant_data(self):
        tz = pytz.timezone('Asia/Kolkata'); now = datetime.now(tz)
        dates = []; candidate = now
        if candidate.hour < 18: candidate -= timedelta(days=1)
        while len(dates) < 2:
            if candidate.weekday() < 5: dates.append(candidate)
            candidate -= timedelta(days=1)
        today, yest = dates[0], dates[1]
        def fetch_oi_csv(date_obj):
            date_str = date_obj.strftime('%d%m%Y')
            url = f"https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv"
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                if r.status_code == 200:
                    content = r.content.decode('utf-8'); lines = content.splitlines()
                    for idx, line in enumerate(lines[:20]):
                        if "Future Index Long" in line:
                            df = pd.read_csv(io.StringIO(content), skiprows=idx); df.columns = df.columns.str.strip(); return df
            except: pass
            return None
        df_today = fetch_oi_csv(today); df_yest = fetch_oi_csv(yest) if df_yest is not None else None
        if df_today is None: return None, None, 0.0, today.strftime('%d-%b-%Y')
        today_data = self._process_participant_data(df_today)
        yest_data = self._process_participant_data(df_yest) if df_yest is not None else {}
        fii_net_change = 0.0
        if today_data.get('FII') and yest_data.get('FII'):
            fii_net_change = today_data['FII'].fut_net - yest_data['FII'].fut_net
        return today_data, yest_data, fii_net_change, today.strftime('%d-%b-%Y')
    def _process_participant_data(self, df) -> Dict[str, ParticipantData]:
        data = {}
        for p in ["FII", "DII", "Client", "Pro"]:
            try:
                row = df[df['Client Type'].astype(str).str.contains(p, case=False, na=False)].iloc[0]
                data[p] = ParticipantData(
                    fut_long=float(row['Future Index Long']), fut_short=float(row['Future Index Short']),
                    fut_net=float(row['Future Index Long']) - float(row['Future Index Short']),
                    call_long=float(row['Option Index Call Long']), call_short=float(row['Option Index Call Short']),
                    call_net=float(row['Option Index Call Long']) - float(row['Option Index Call Short']),
                    put_long=float(row['Option Index Put Long']), put_short=float(row['Option Index Put Short']),
                    put_net=float(row['Option Index Put Long']) - float(row['Option Index Put Short']),
                    stock_net=float(row['Future Stock Long']) - float(row['Future Stock Short'])
                )
            except: data[p] = None
        return data
    def get_time_metrics(self, weekly, monthly, next_weekly) -> TimeMetrics:
        today = date.today()
        dte_w = (weekly - today).days if weekly```python
        else 0
        dte_m = (monthly - today).days if monthly else 0
        dte_nw = (next_weekly - today).days if next_weekly else 0
        return TimeMetrics(today, weekly, monthly, next_weekly, dte_w, dte_m, dte_w <= ProductionConfig.GAMMA_DANGER_DTE, dte_m <= ProductionConfig.GAMMA_DANGER_DTE, dte_nw)
    def get_vol_metrics(self, nifty_hist, vix_hist, live_prices) -> VolMetrics:
        is_fallback = False
        nifty_live = vix_live = 0
        if hasattr(live_prices, 'data'):
            data = live_prices.data
            if ProductionConfig.NIFTY_KEY in data: nifty_live = data[ProductionConfig.NIFTY_KEY].last_price
            if ProductionConfig.VIX_KEY in data: vix_live = data[ProductionConfig.VIX_KEY].last_price
        spot = nifty_live if nifty_live > 0 else (nifty_hist.iloc[-1]['close'] if not nifty_hist.empty else 0)
        vix = vix_live if vix_live > 0 else (vix_hist.iloc[-1]['close'] if not vix_hist.empty else 0)
        if nifty_live <= 0 or vix_live <= 0: is_fallback = True
        returns = np.log(nifty_hist['close'] / nifty_hist['close'].shift(1)).dropna()
        rv7 = returns.rolling(7).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 7 else 0
        rv28 = returns.rolling(28).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 28 else 0
        rv90 = returns.rolling(90).std().iloc[-1] * np.sqrt(252) * 100 if len(returns) >= 90 else 0
        def fit_garch(horizon):
            try:
                if len(returns) < 100: return 0
                model = arch_model(returns * 100, vol='Garch', p=1, q=1, dist='normal')
                result = model.fit(disp='off', show_warning=False)
                forecast = result.forecast(horizon=horizon, reindex=False)
                return np.sqrt(forecast.variance.values[-1, -1]) * np.sqrt(252)
            except: return 0
        garch7 = fit_garch(7) or rv7; garch28 = fit_garch(28) or rv28
        const = 1.0 / (4.0 * np.log(2.0))
        park7 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(7).mean() * const) * np.sqrt(252) * 100 if len(nifty_hist) >= 7 else 0
        park28 = np.sqrt((np.log(nifty_hist['high'] / nifty_hist['low']) ** 2).tail(28).mean() * const) * np.sqrt(252) * 100 if len(nifty_hist) >= 28 else 0
        vix_returns = np.log(vix_hist['close'] / vix_hist['close'].shift(1)).dropna()
        vov = vix_returns.rolling(30).std().iloc[-1] * np.sqrt(252) * 100 if len(vix_returns) >= 30 else 0
        vov_rolling = vix_returns.rolling(30).std() * np.sqrt(252) * 100 if len(vix_returns) >= 30 else pd.Series()
        vov_mean = vov_rolling.rolling(60).mean().iloc[-1] if len(vov_rolling) >= 60 else 0
        vov_std = vov_rolling.rolling(60).std().iloc[-1] if len(vov_rolling) >= 60 else 0
        vov_zscore = (vov - vov_mean) / vov_std if vov_std > 0 else 0
        def calc_ivp(window):
            if len(vix_hist) < window: return 0.0
            history = vix_hist['close'].tail(window)
            return (history < vix).mean() * 100
        ivp_30d, ivp_90d, ivp_1yr = calc_ivp(30), calc_ivp(90), calc_ivp(252)
        ma20 = nifty_hist['close'].rolling(20).mean().iloc[-1] if len(nifty_hist) >= 20 else 0
        true_range = pd.concat([nifty_hist['high'] - nifty_hist['low'], (nifty_hist['high'] - nifty_hist['close'].shift(1)).abs(), (nifty_hist['low'] - nifty_hist['close'].shift(1)).abs()], axis=1).max(axis=1)
        atr14 = true_range.rolling(14).mean().iloc[-1] if len(true_range) >= 14 else 0
        trend_strength = abs(spot - ma20) / atr14 if atr14 > 0 else 0
        vol_regime = "EXPLODING" if vov_zscore > ProductionConfig.VOV_CRASH_ZSCORE else "RICH" if ivp_1yr > ProductionConfig.HIGH_VOL_IVP else "CHEAP" if ivp_1yr < ProductionConfig.LOW_VOL_IVP else "FAIR"
        return VolMetrics(spot, vix, rv7, rv28, rv90, garch7, garch28, park7, park28, vov, vov_zscore, ivp_30d, ivp_90d, ivp_1yr, ma20, atr14, trend_strength, vol_regime, is_fallback)
    def get_struct_metrics(self, chain, spot, lot_size) -> StructMetrics:
        if chain.empty or spot == 0: return StructMetrics(0, 0, 0, "NEUTRAL", 0, 0, 0, "NEUTRAL", lot_size)
        subset = chain[(chain['strike'] > spot * 0.90) & (chain['strike'] < spot * 1.10)]
        net_gex = ((subset['ce_gamma'] * subset['ce_oi']).sum() - (subset['pe_gamma'] * subset['pe_oi']).sum()) * spot * lot_size
        total_oi_value = (chain['ce_oi'].sum() + chain['pe_oi'].sum()) * spot * lot_size
        gex_ratio = abs(net_gex) / total_oi_value if total_oi_value > 0 else 0
        gex_regime = "STICKY" if gex_ratio > ProductionConfig.GEX_STICKY_RATIO else "SLIPPERY" if gex_ratio < ProductionConfig.GEX_STICKY_RATIO * 0.5 else "NEUTRAL"
        pcr = chain['pe_oi'].sum() / chain['ce_oi'].sum() if chain['ce_oi'].sum() > 0 else 1.0
        strikes = chain['strike'].values
        losses = [np.sum(np.maximum(0, s - strikes) * chain['ce_oi'].values) + np.sum(np.maximum(0, strikes - s) * chain['pe_oi'].values) for s in strikes]
        max_pain = strikes[np.argmin(losses)] if losses else 0
        try:
            ce_25d_idx = (chain['ce_delta'].abs() - 0.25).abs().argsort()[:1]
            pe_25d_idx = (chain['pe_delta'].abs() - 0.25).abs().argsort()[:1]
            skew_25d = chain.iloc[pe_25d_idx]['pe_iv'].values[0] - chain.iloc[ce_25d_idx]['ce_iv'].values[0]
        except: skew_25d = 0
        oi_regime = "BULLISH" if pcr > 1.2 else "BEARISH" if pcr < 0.8 else "NEUTRAL"
        return StructMetrics(net_gex, gex_ratio, total_oi_value, gex_regime, pcr, max_pain, skew_25d, oi_regime, lot_size)
    def get_edge_metrics(self, weekly_chain, monthly_chain, spot, vol: VolMetrics) -> EdgeMetrics:
        def get_atm_iv(chain):
            if chain.empty or spot == 0: return 0
            atm_idx = (chain['strike'] - spot).abs().argsort()[:1]; row = chain.iloc[atm_idx].iloc[0]; return (row['ce_iv'] + row['pe_iv']) / 2
        iv_weekly = get_atm_iv(weekly_chain); iv_monthly = get_atm_iv(monthly_chain)
        vrp_rv_weekly = iv_weekly - vol.rv7; vrp_garch_weekly = iv_weekly - vol.garch7; vrp_park_weekly = iv_weekly - vol.park7
        vrp_rv_monthly = iv_monthly - vol.rv28; vrp_garch_monthly = iv_monthly - vol.garch28; vrp_park_monthly = iv_monthly - vol.park28
        term_spread = iv_monthly - iv_weekly
        term_regime = "BACKWARDATION" if term_spread < -1.0 else "CONTANGO" if term_spread > 1.0 else "FLAT"
        primary_edge = "LONG_VOL" if vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP else "SHORT_GAMMA" if vrp_park_weekly > 4.0 and vol.ivp_1yr > 50 else "SHORT_VEGA" if vrp_park_monthly > 3.0 and vol.ivp_1yr > 50 else "CALENDAR_SPREAD" if term_regime == "BACKWARDATION" and term_spread < -2.0 else "MEAN_REVERSION" if vol.ivp_1yr > ProductionConfig.HIGH_VOL_IVP else "NONE"
        return EdgeMetrics(iv_weekly, vrp_rv_weekly, vrp_garch_weekly, vrp_park_weekly, iv_monthly, vrp_rv_monthly, vrp_garch_monthly, vrp_park_monthly, term_spread, term_regime, primary_edge)
    def get_external_metrics(self, nifty_hist, participant_data, participant_yest, fii_net_change, data_date) -> ExternalMetrics:
        fast_vol = False
        if not nifty_hist.empty:
            last_bar = nifty_hist.iloc[-1]
            daily_range_pct = ((last_bar['high'] - last_bar['low']) / last_bar['open']) * 100
            fast_vol = daily_range_pct > 1.8
        flow_regime = "NEUTRAL"
        if participant_data and participant_data.get('FII'):
            fii_net = participant_data['FII'].fut_net
            if fii_net > ProductionConfig.FII_STRONG_LONG: flow_regime = "STRONG_LONG"
            elif fii_net < ProductionConfig.FII_STRONG_SHORT: flow_regime = "STRONG_SHORT"
            elif abs(fii_net) > ProductionConfig.FII_MODERATE: flow_regime = "MODERATE_LONG" if fii_net > 0 else "MODERATE_SHORT"
        event_risk = "LOW"
        return ExternalMetrics(fii=participant_data.get('FII') if participant_data else None,
                               dii=participant_data.get('DII') if participant_data else None,
                               pro=participant_data.get('Pro') if participant_data else None,
                               client=participant_data.get('Client') if participant_data else None,
                               fii_net_change=fii_net_change, flow_regime=flow_regime, fast_vol=fast_vol, data_date=data_date, event_risk=event_risk)

# ----------  REGIME ENGINE  ----------
class RegimeEngine:
    def calculate_scores(self, vol: VolMetrics, struct: StructMetrics, edge: EdgeMetrics, external: ExternalMetrics, time: TimeMetrics, expiry_type: str) -> RegimeScore:
        if expiry_type == "WEEKLY":
            w_vrp = (edge.vrp_garch_weekly * 0.7) + (edge.vrp_park_weekly * 0.15) + (edge.vrp_rv_weekly * 0.15)
        else:
            w_vrp = (edge.vrp_garch_monthly * 0.7) + (edge.vrp_park_monthly * 0.15) + (edge.vrp_rv_monthly * 0.15)
        edge_score = 5.0
        if w_vrp > 4.0: edge_score += 3.0
        elif w_vrp > 2.0: edge_score += 2.0
        elif w_vrp > 1.0: edge_score += 1.0
        elif w_vrp < 0: edge_score -= 3.0
        if edge.term_regime == "BACKWARDATION" and edge.term_spread < -2.0: edge_score += 1.0
        elif edge.term_regime == "CONTANGO": edge_score += 0.5
        edge_score = max(0, min(10, edge_score))
        vol_score = 5.0
        if vol.vov_zscore > ProductionConfig.VOV_CRASH_ZSCORE: vol_score = 0.0
        elif vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE: vol_score -= 3.0
        elif vol.vov_zscore < 1.5: vol_score += 1.5
        if vol.ivp_1yr > ProductionConfig.HIGH_VOL_IVP: vol_score += 0.5
        elif vol.ivp_1yr < ProductionConfig.LOW_VOL_IVP: vol_score -= 2.5
        else: vol_score += 1.0
        vol_score = max(0, min(10, vol_score))
        struct_score = 5.0
        if struct.gex_regime == "STICKY": struct_score += 2.5 if expiry_type == "WEEKLY" and time.dte_weekly <= 1 else 1.0
        elif struct.gex_regime == "SLIPPERY": struct_score -= 1.0
        if 0.9 < struct.pcr < 1.1: struct_score += 1.0
        elif struct.pcr > 1.3 or struct.pcr < 0.7: struct_score -= 0.5
        if abs(struct.skew_25d) > 3.0: struct_score -= 0.5
        struct_score = max(0, min(10, struct_score))
        risk_score = 10.0
        if external.event_risk == "HIGH": risk_score -= 3.0
        elif external.event_risk == "MEDIUM": risk_score -= 1.5
        if external.fast_vol: risk_score -= 2.0
        if external.flow_regime == "STRONG_SHORT": risk_score -= 3.0
        elif external.flow_regime == "STRONG_LONG": risk_score += 1.0
        if expiry_type == "WEEKLY" and time.is_gamma_week: risk_score -= 2.0
        elif expiry_type == "MONTHLY" and time.is_gamma_month: risk_score -= 2.5
        risk_score = max(0, min(10, risk_score))
        composite = (vol_score * ProductionConfig.WEIGHT_VOL + struct_score * ProductionConfig.WEIGHT_STRUCT + edge_score * ProductionConfig.WEIGHT_EDGE + risk_score * ProductionConfig.WEIGHT_RISK)
        confidence = "VERY_HIGH" if composite >= 8.0 else "HIGH" if composite >= 6.5 else "MODERATE" if composite >= 4.0 else "LOW"
        return RegimeScore(vol_score, struct_score, edge_score, risk_score, composite, confidence)
    def generate_mandate(self, score: RegimeScore, vol: VolMetrics, struct: StructMetrics, edge: EdgeMetrics, external: ExternalMetrics, time: TimeMetrics, expiry_type: str, expiry_date: date, dte: int) -> TradingMandate:
        if expiry_type == "WEEKLY":
            w_vrp = (edge.vrp_garch_weekly * 0.7) + (edge.vrp_park_weekly * 0.15) + (edge.vrp_rv_weekly * 0.15)
        else:
            w_vrp = (edge.vrp_garch_monthly * 0.7) + (edge.vrp_park_monthly * 0.15) + (edge.vrp_rv_monthly * 0.15)
        if score.composite >= 7.5 and dte > 2:
            regime_name = "AGGRESSIVE_SHORT"; allocation = 60.0; strategy = "AGGRESSIVE_SHORT"; suggested = "IRON_CONDOR"
        elif score.composite >= 7.5 and dte <= 2:
            regime_name = "AGGRESSIVE_SHORT_GAMMA"; allocation = 50.0; strategy = "AGGRESSIVE_SHORT"; suggested = "IRON_FLY"
        elif score.composite >= 6.0 and dte > 1:
            regime_name = "MODERATE_SHORT"; allocation = 40.0; strategy = "MODERATE_SHORT"; suggested = "IRON_CONDOR"
        elif score.composite >= 6.0 and dte <= 1:
            regime_name = "MODERATE_SHORT_GAMMA"; allocation = 35.0; strategy = "MODERATE_SHORT"; suggested = "IRON_FLY"
        elif score.composite >= 4.0:
            regime_name = "DEFENSIVE"; allocation = 20.0; strategy = "DEFENSIVE"; suggested = "CREDIT_SPREAD"
        else:
            regime_name = "CASH"; allocation = 0.0; strategy = "CASH"; suggested = "NONE"
        if vol.vov_zscore > ProductionConfig.VOV_WARNING_ZSCORE:
            warnings = [f"⚠️  HIGH VOL-OF-VOL ({vol.vov_zscore:.2f}σ)"]; allocation *= 0.7
        else: warnings = []
        if external.flow_regime == "STRONG_SHORT" and external.fii:
            warnings.append("⚠️  FII DUMPING"); allocation = min(allocation, 30.0)
        try:
            daily_regime = db_writer.get_state("daily_risk_regime")
            if daily_regime:
                ai_data = json.loads(daily_regime)
                if ai_data.get("date") == str(date.today()):
                    risk_score = ai_data.get("risk_score", 5)
                    if risk_score >= 8:
                        strategy = "DEFENSIVE"; suggested = "CASH_ONLY"; allocation = 0.0
                        warnings.append(f"⛔ AI BRAKE: Extreme Risk ({risk_score}/10) - HALTED")
                    elif risk_score >= 6:
                        allocation = allocation * 0.5
                        warnings.append(f"⚠️  AI CAUTION: Risk ({risk_score}/10) - Sizing Halved")
        except: pass
        deployable = ProductionConfig.BASE_CAPITAL * (allocation / 100.0)
        risk_per_lot = ProductionConfig.MARGIN_SELL_BASE if strategy != "DEFENSIVE" else ProductionConfig.MARGIN_SELL_BASE * 0.6
        max_lots = int(deployable / risk_per_lot) if risk_per_lot > 0 else 0
        return TradingMandate(expiry_type, expiry_date, dte, regime_name, strategy, allocation, max_lots, risk_per_lot, score, [f"Weighted VRP {w_vrp:.2f}"], warnings, suggested)

# ----------  STRATEGY FACTORY  ----------
class StrategyFactory:
    def __init__(self, api_client: upstox_client.ApiClient): self.api_client = api_client
    def _discover_strike_interval(self, df: pd.DataFrame) -> int:
        if df.empty or len(df) < 2: return ProductionConfig.DEFAULT_STRIKE_INTERVAL
        strikes = sorted(df['strike'].unique()); diffs = np.diff(strikes); valid_diffs = diffs[diffs > 0]
        if len(valid_diffs) == 0: return ProductionConfig.DEFAULT_STRIKE_INTERVAL
        try: return int(pd.Series(valid_diffs).mode().iloc[0])
        except: return ProductionConfig.DEFAULT_STRIKE_INTERVAL
    def _find_professional_atm(self, df: pd.DataFrame, spot: float) -> Optional[Dict]:
        interval = self._discover_strike_interval(df)
        closest = int(spot / interval + 0.5) * interval
        candidates = [closest, closest + interval, closest - interval]
        best_strike, min_skew, best_cost = None, float('inf'), 0.0
        for strike in candidates:
            ce = df[(df['strike'] == strike) & (df['ce_oi'] > ProductionConfig.MIN_STRIKE_OI)]
            pe = df[(df['strike'] == strike) & (df['pe_oi'] > ProductionConfig.MIN_STRIKE_OI)]
            if ce.empty or pe.empty: continue
            ce_ltp, pe_ltp = ce.iloc[0]['ce_ltp'], pe.iloc[0]['pe_ltp']
            if ce_ltp <= 0.1 or pe_ltp <= 0.1: continue
            skew = abs(ce_ltp - pe_ltp)
            if skew < min_skew: min_skew, best_strike, best_cost = skew, strike, ce_ltp + pe_ltp
        if not best_strike:
            logger.warning(f"Using Geometric ATM {closest} (Liquidity Low)")
            return {'strike': closest, 'straddle_cost': 0.0, 'interval': interval}
        logger.info(f"🎯 Pro ATM: {best_strike} (Skew: ₹{min_skew:.1f}) | Interval: {interval}")
        return {'strike': best_strike, 'straddle_cost': best_cost, 'interval': interval}
    def _calculate_pro_wing_width(self, spot: float, vol_metrics: VolMetrics, dte: int, interval: int) -> int:
        annual_vol = vol_metrics.garch7
        expected_move = spot * (annual_vol / 100) * np.sqrt(dte / 365)
        if vol_metrics.ivp_1yr > ProductionConfig.IVP_THRESHOLD_EXTREME: factor = ProductionConfig.WING_FACTOR_EXTREME_VOL
        elif vol_metrics.ivp_1yr > ProductionConfig.IVP_THRESHOLD_HIGH: factor = ProductionConfig.WING_FACTOR_HIGH_VOL
        elif vol_metrics.ivp_1yr < ProductionConfig.IVP_THRESHOLD_LOW: factor = ProductionConfig.WING_FACTOR_LOW_VOL
        else: factor = ProductionConfig.WING_FACTOR_STANDARD
        target_width = expected_move * factor
        rounded_width = int(target_width / interval + 0.5) * interval
        min_width = interval * ProductionConfig.MIN_WING_INTERVAL_MULTIPLIER
        final_width = max(min_width, rounded_width)
        logger.info(f"📏 Wing Width Calculation: Spot={spot:.2f} Vol={annual_vol:.2f}% DTE={dte} → Expected Move={expected_move:.2f} → Final Width={final_width}")
        return final_width
    def _get_leg_details(self, df: pd.DataFrame, strike: float, type_: str) -> Optional[Dict]:
        rows = df[(df['strike'] - strike).abs() < 0.1]
        if rows.empty: return None
        row = rows.iloc[0]; pref = type_.lower(); ltp = row[f'{pref}_ltp']
        if ltp <= 0: return None
        return {'key': row[f'{pref}_key'], 'strike': row['strike'], 'ltp': ltp, 'delta': row[f'{pref}_delta'], 'type': type_,
                'bid': row[f'{pref}_bid'], 'ask': row[f'{pref}_ask']}
    def _find_leg_by_delta(self, df: pd.DataFrame, type_: str, target_delta: float) -> Optional[Dict]:
        target, col_delta = abs(target_delta), f"{type_.lower()}_delta"
        df = df.copy()
        df = df[(df[f'{type_.lower()}_oi'] > ProductionConfig.MIN_STRIKE_OI) & (df[f'{type_.lower()}_ltp'] > 0.5)]
        df['delta_diff'] = (df[col_delta].abs() - target).abs()
        for _, row in df.sort_values('delta_diff').head(3).iterrows():
            bid, ask, ltp = row[f'{type_.lower()}_bid'], row[f'{type_.lower()}_ask'], row[f'{type_.lower()}_ltp']
            if ltp <= 0 or ask <= 0: continue
            if (ask - bid) / ltp > ProductionConfig.MAX_BID_ASK_SPREAD: continue
            return self._get_leg_details(df, row['strike'], type_)
        return None
    def _calculate_defined_risk(self, legs: List[Dict], qty: int) -> float:
        if not legs: return 0.0
        premiums = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
        debits = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'BUY')
        net_credit = premiums - debits
        ce_legs = sorted([l for l in legs if l['type'] == 'CE'], key=lambda x: x['strike'])
        pe_legs = sorted([l for l in legs if l['type'] == 'PE'], key=lambda x: x['strike'])
        call_risk = 0.0; put_risk = 0.0
        if len(ce_legs) >= 2:
            shorts = [l for l in ce_legs if l['side'] == 'SELL']; longs = [l for l in ce_legs if l['side'] == 'BUY']
            if shorts and longs: width = longs[-1]['strike'] - shorts[0]['strike']; call_risk = width * qty
        if len(pe_legs) >= 2:
            shorts = [l for l in pe_legs if l['side'] == 'SELL']; longs = [l for l in pe_legs if l['side'] == 'BUY']
            if shorts and longs: width = shorts[-1]['strike'] - longs[0]['strike']; put_risk = width * qty
        max_structural_risk = max(call_risk, put_risk)
        max_loss = max(0, max_structural_risk - net_credit)
        logger.info(f"🧮 Risk Calc: CallRisk={call_risk:.0f}, PutRisk={put_risk:.0f}, Credit={net_credit:.0f} → MaxLoss={max_loss:.0f}")
        return max_loss
    def generate(self, mandate: TradingMandate, chain: pd.DataFrame, lot_size: int, vol_metrics: VolMetrics, spot: float, struct_metrics: StructMetrics) -> Tuple[List[Dict], float]:
        if mandate.max_lots == 0 or chain.empty: return [], 0.0
        qty = mandate.max_lots * lot_size; legs = []
        if mandate.suggested_structure == "IRON_FLY":
            logger.info(f"🦅 Constructing Iron Fly | DTE={mandate.dte} | Spot={spot:.2f}")
            atm_data = self._find_professional_atm(chain, spot)
            if not atm_data: return [], 0.0
            atm_strike, straddle_cost, interval = atm_data['strike'], atm_data['straddle_cost'], atm_data['interval']
            wing_width = self._calculate_pro_wing_width(spot, vol_metrics, mandate.dte, interval)
            upper_wing, lower_wing = atm_strike + wing_width, atm_strike - wing_width
            atm_call = self._get_leg_details(chain, atm_strike, 'CE'); atm_put = self._get_leg_details(chain, atm_strike, 'PE')
            wing_call = self._get_leg_details(chain, upper_wing, 'CE'); wing_put = self._get_leg_details(chain, lower_wing, 'PE')
            if not all([atm_call, atm_put, wing_call, wing_put]): logger.error("Iron Fly incomplete: Missing liquid strikes"); return [], 0.0
            legs = [
                {**atm_call, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'IRON_FLY'},
                {**atm_put,  'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'IRON_FLY'},
                {**wing_call,'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'IRON_FLY'},
                {**wing_put, 'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'IRON_FLY'}
            ]
        elif mandate.suggested_structure == "IRON_CONDOR":
            logger.info(f"🦅 Constructing Iron Condor | DTE={mandate.dte}")
            short_delta = ProductionConfig.DELTA_SHORT_MONTHLY if mandate.expiry_type == "MONTHLY" else ProductionConfig.DELTA_SHORT_WEEKLY
            legs = [
                self._find_leg_by_delta(chain, 'CE', short_delta),
                self._find_leg_by_delta(chain, 'PE', short_delta),
                self._find_leg_by_delta(chain, 'CE', ProductionConfig.DELTA_LONG_HEDGE),
                self._find_leg_by_delta(chain, 'PE', ProductionConfig.DELTA_LONG_HEDGE)
            ]
            if not all(legs): logger.error("Iron Condor incomplete"); return [], 0.0
            legs = [{**l, 'side': 'SELL' if idx < 2 else 'BUY', 'role': 'CORE' if idx < 2 else 'HEDGE', 'qty': qty, 'structure': 'IRON_CONDOR'} for idx, l in enumerate(legs)]
        elif mandate.suggested_structure == "CREDIT_SPREAD":
            is_uptrend = vol_metrics.spot > vol_metrics.ma20 * (1 + ProductionConfig.TREND_BULLISH_THRESHOLD/100)
            is_bullish_pcr = struct_metrics.pcr > ProductionConfig.PCR_BULLISH_THRESHOLD
            if is_uptrend:
                logger.info("📈 Direction: BULLISH (Spot > MA20). Deploying BULL PUT SPREAD.")
                short = self._find_leg_by_delta(chain, 'PE', ProductionConfig.DELTA_CREDIT_SHORT)
                long  = self._find_leg_by_delta(chain, 'PE', ProductionConfig.DELTA_CREDIT_LONG)
                if not all([short, long]): return [], 0.0
                legs = [
                    {**short, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'BULL_PUT_SPREAD'},
                    {**long,  'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'BULL_PUT_SPREAD'}
                ]
            else:
                logger.info("📉 Direction: BEARISH (Spot ≤ MA20). Deploying BEAR CALL SPREAD.")
                short = self._find_leg_by_delta(chain, 'CE', ProductionConfig.DELTA_CREDIT_SHORT)
                long  = self._find_leg_by_delta(chain, 'CE', ProductionConfig.DELTA_CREDIT_LONG)
                if not all([short, long]): return [], 0.0
                legs = [
                    {**short, 'side': 'SELL', 'role': 'CORE', 'qty': qty, 'structure': 'BEAR_CALL_SPREAD'},
                    {**long,  'side': 'BUY',  'role': 'HEDGE','qty': qty, 'structure': 'BEAR_CALL_SPREAD'}
                ]
        if not legs: return [], 0.0
        for leg in legs:
            if leg['ltp'] <= 0:
                logger.error(f"❌ Invalid Leg Price: {leg['strike']} = {leg['ltp']}")
                return [], 0.0
        max_risk = self._calculate_defined_risk(legs, qty)
        if max_risk > ProductionConfig.MAX_LOSS_PER_TRADE:
            logger.critical(f"⛔ Trade Rejected: Max Risk ₹{max_risk:,.2f} > Limit ₹{ProductionConfig.MAX_LOSS_PER_TRADE:,.2f}")
            telegram.send(f"Trade Rejected: Risk ₹{max_risk:,.0f} exceeds limit", "WARNING")
            return [], 0.0
        return legs, max_risk

# ----------  EXECUTION ENGINE  ----------
class ExecutionEngine:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.order_updates = {}; self.update_lock = threading.Lock()
        self.validator = InstrumentValidator(api_client)
        if not ProductionConfig.DRY_RUN_MODE: self._setup_portfolio_stream()
        else: logger.info("📄 Dry run mode - skipping WebSocket setup")
    def _setup_portfolio_stream(self):
        try:
            self.portfolio_streamer = upstox_client.PortfolioDataStreamer(self.api_client, order_update=True, position_update=True, holding_update=False, gtt_update=True)
            def on_message(message):
                with self.update_lock:
                    if 'order_updates' in message:
                        for update in message['order_updates']:
                            order_id = update.get('order_id')
                            if order_id: self.order_updates[order_id] = update
            def on_open(): logger.info("✅ Portfolio Stream Connected")
            def on_error(error): logger.error(f"Portfolio Stream Error: {error}")
            def on_close(): logger.warning("Portfolio Stream Closed")
            self.portfolio_streamer.on("message", on_message); self.portfolio_streamer.on("open", on_open)
            self.portfolio_streamer.on("error", on_error); self.portfolio_streamer.on("close", on_close)
            self.portfolio_streamer.auto_reconnect(True, 10, 5)
            threading.Thread(target=self.portfolio_streamer.connect, daemon=True, name="Portfolio-WS").start()
            time.sleep(2)
        except Exception as e: logger.error(f"Failed to setup portfolio stream: {e}")
    def check_margin_requirement(self, legs: List[Dict]) -> float:
        if not api_rate_limiter.acquire(): return float('inf')
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                charge_api = ChargeApi(self.api_client)
                instruments = [upstox_client.Instrument(instrument_key=leg['key'], quantity=int(leg['qty']), transaction_type=leg['side'], product="D") for leg in legs]
                margin_request = upstox_client.MarginRequest(instruments=instruments)
                response = charge_api.post_margin(margin_request)
                if response.status == 'success' and hasattr(response.data, 'required_margin'):
                    margin = float(response.data.required_margin)
                    logger.info(f"Margin requirement: ₹{margin:,.2f}")
                    return margin
            except Exception as e:
                logger.error(f"Margin check error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(2 ** attempt)
        return float('inf')
    def get_funds(self) -> float:
        if not api_rate_limiter.acquire(): return 0.0
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                user_api = upstox_client.UserApi(self.api_client)
                response = user_api.get_user_fund_margin(api_version="2.0")
                if response.status == 'success' and response.data and hasattr(response.data, 'equity') and hasattr(response.data.equity, 'available_margin'):
                    funds = float(response.data.equity.available_margin)
                    logger.info(f"Available funds: ₹{funds:,.2f}")
                    return funds
            except Exception as e:
                logger.error(f"Funds fetch error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(2 ** attempt)
        return 0.0
    def place_order(self, instrument_key: str, qty: int, side: str, order_type: str = "LIMIT", price: float = 0.0) -> Optional[str]:
        if qty <= 0 or price < 0: logger.error(f"Invalid order parameters: qty={qty}, price={price}"); return None
        if ProductionConfig.DRY_RUN_MODE: return paper_engine.place_order(instrument_key, qty, side, order_type, price)
        if not self.validator.validate_contract_exists(instrument_key): logger.error(f"Contract validation failed: {instrument_key}"); return None
        if self.validator.is_instrument_banned(instrument_key): logger.error(f"Instrument is banned: {instrument_key}"); telegram.send(f"⛔ Attempted trade on banned instrument: {instrument_key}", "ERROR"); return None
        if not api_rate_limiter.acquire(): return None
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                body = upstox_client.PlaceOrderV3Request(quantity=int(qty), product="D", validity="DAY", price=float(price), tag="VG30", instrument_token=instrument_key,
                                                        order_type=order_type, transaction_type=side, disclosed_quantity=0, trigger_price=0.0, is_amo=False, slice=True)
                response = order_api.place_order(body)
                if response.status == 'success' and response.data and hasattr(response.data, 'order_ids') and response.data.order_ids:
                    order_id = response.data.order_ids[0]
                    logger.info(f"ORDER PLACED: {side} {qty}x {instrument_key} @ {price} | ID={order_id}")
                    db_writer.log_order(order_id, instrument_key, side, qty, price, "PLACED")
                    return order_id
            except Exception as e:
                logger.error(f"Order placement error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(1)
        logger.error(f"Order placement failed after {ProductionConfig.MAX_API_RETRIES} attempts")
        db_writer.log_order("FAILED", instrument_key, side, qty, price, "FAILED", message="All retries exhausted")
        return None
    def get_order_status(self, order_id: str) -> Optional[Dict]:
        if ProductionConfig.DRY_RUN_MODE: return paper_engine.get_order_status(order_id)
        with self.update_lock:
            if order_id in self.order_updates:
                update = self.order_updates[order_id]
                return {'status': update.get('status', '').lower(), 'avg_price': float(update.get('average_price', 0)), 'filled_qty': int(update.get('filled_quantity', 0))}
        if not api_rate_limiter.acquire(): return None
        try:
            order_api = OrderApi(self.api_client)
            response = order_api.get_order_details(api_version="2.0", order_id=order_id)
            if response.status == 'success' and response.data:
                order_data = response.data
                return {'status': order_data.status.lower() if hasattr(order_data, 'status') else 'unknown',
                        'avg_price': float(order_data.average_price) if hasattr(order_data, 'average_price') and order_data.average_price else 0.0,
                        'filled_qty': int(order_data.filled_quantity) if hasattr(order_data, 'filled_quantity') and order_data.filled_quantity else 0}
        except Exception as e: logger.error(f"Order status check failed: {e}"); return None
    def cancel_order(self, order_id: str) -> bool:
        if ProductionConfig.DRY_RUN_MODE: return paper_engine.cancel_order(order_id)
        if not api_rate_limiter.acquire(): return False
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                order_api.cancel_order(order_id=order_id)
                logger.info(f"ORDER CANCELLED: {order_id}")
                db_writer.log_order(order_id, "", "", 0, 0, "CANCELLED")
                return True
            except Exception as e:
                logger.error(f"Cancel order error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(0.5)
        return False
    def place_gtt_order(self, instrument_key: str, qty: int, side: str, stop_loss_price: float, target_price: float) -> Optional[str]:
        if qty <= 0 or stop_loss_price <= 0 or target_price <= 0: logger.error("Invalid GTT parameters"); return None
        if not api_rate_limiter.acquire(): return None
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                sl_trigger = "BELOW" if side == "BUY" else "ABOVE"
                rules = [upstox_client.GttRule(strategy="STOPLOSS", trigger_type=sl_trigger, trigger_price=float(stop_loss_price)),
                         upstox_client.GttRule(strategy="TARGET", trigger_type="IMMEDIATE", trigger_price=float(target_price))]
                body = upstox_client.GttPlaceOrderRequest(type="MULTIPLE", quantity=int(qty), product="D", rules=rules, instrument_token=instrument_key, transaction_type=side)
                response = order_api.place_gtt_order(body)
                if response.status == 'success' and response.data and hasattr(response.data, 'gtt_order_ids') and response.data.gtt_order_ids:
                    gtt_id = response.data.gtt_order_ids[0]
                    logger.info(f"GTT PLACED: {side} {qty}x {instrument_key} | SL={stop_loss_price} Target={target_price} | ID={gtt_id}")
                    return gtt_id
            except Exception as e:
                logger.error(f"GTT placement error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(1)
        logger.error("GTT placement failed after all retries"); return None
    def get_gtt_order_details(self, gtt_id: str) -> Optional[str]:
        if not api_rate_limiter.acquire(): return None
        try:
            order_api = OrderApiV3(self.api_client)
            response = order_api.get_gtt_order_details(gtt_order_id=gtt_id)
            if response.status == 'success' and response.data:
                data = response.data[0] if isinstance(response.data, list) else response.data
                return data.status if hasattr(data, 'status') else None
            return None
        except Exception as e: logger.error(f"GTT check failed: {e}"); return None
    def cancel_gtt_order(self, gtt_id: str) -> bool:
        if not api_rate_limiter.acquire(): return False
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApiV3(self.api_client)
                order_api.cancel_gtt_order(gtt_order_id=gtt_id)
                logger.info(f"GTT CANCELLED: {gtt_id}"); return True
            except Exception as e:
                logger.error(f"GTT cancel error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(0.5)
        return False
    def get_brokerage_impact(self, legs: List[Dict]) -> float:
        if not api_rate_limiter.acquire(): return 0.0
        try:
            charge_api = ChargeApi(self.api_client); total_brokerage = 0.0
            for leg in legs:
                response = charge_api.get_brokerage(leg['key'], leg['qty'], 'D', leg['side'], leg['ltp'], "2.0")
                if response.status == 'success' and response.data and hasattr(response.data, 'charges'):
                    total_brokerage += float(response.data.charges.total)
            logger.info(f"Estimated brokerage: ₹{total_brokerage:.2f}")
            return total_brokerage
        except Exception as e: logger.error(f"Brokerage calculation error: {e}"); return 0.0
    def exit_all_positions(self, tag: Optional[str] = None) -> bool:
        if not api_rate_limiter.acquire(): return False
        for attempt in range(ProductionConfig.MAX_API_RETRIES):
            try:
                order_api = OrderApi(self.api_client)
                response = order_api.exit_positions()
                if response.status == 'success':
                    logger.critical("🚨 ATOMIC EXIT EXECUTED")
                    telegram.send("🚨 Server-side atomic exit completed", "CRITICAL")
                    return True
            except Exception as e:
                logger.error(f"Atomic exit error (attempt {attempt+1}): {e}")
                if attempt < ProductionConfig.MAX_API_RETRIES - 1: time.sleep(1)
        return False
    def verify_gtt(self, gtt_ids: List[str]) -> bool:
        if not api_rate_limiter.acquire(): return False
        try:
            for gtt_id in gtt_ids:
                status = self.get_gtt_order_details(gtt_id)
                if status != 'active':
                    logger.warning(f"GTT {gtt_id} status: {status}")
                    telegram.send(f"⚠️ GTT verification failed: {gtt_id} is {status}", "WARNING")
                    return False
            logger.info(f"✅ All GTTs verified: {len(gtt_ids)} orders active")
            return True
        except Exception as e: logger.error(f"GTT verification failed: {e}"); return False
    def _smart_limit_price(self, leg: Dict, market_depth: Dict = None) -> float:
        ltp = leg['ltp']; side = leg['side']; role = leg['role']
        bid = leg.get('bid', ltp * 0.995); ask = leg.get('ask', ltp * 1.005)
        spread_pct = (ask - bid) / ltp if ltp > 0 else 0.01
        is_opening = dtime(9, 15) <= datetime.now(ProductionConfig.IST).time() <= dtime(9, 30)
        is_closing = dtime(15, 15) <= datetime.now(ProductionConfig.IST).time() <= dtime(15, 30)
        base_tolerance = 0.003 if role == 'HEDGE' else 0.002
        if spread_pct > 0.02: tolerance = base_tolerance * 1.5
        else: tolerance = base_tolerance
        if is_opening or is_closing: tolerance *= 1.3
        if side == 'BUY':
            mid_price = (bid + ask) / 2
            limit_price = min(mid_price * (1 + tolerance), ask * (1 + tolerance))
        else:
            mid_price = (bid + ask) / 2
            limit_price = max(mid_price * (1 - tolerance), bid * (1 - tolerance))
        limit_price = round(limit_price * 20) / 20
        logger.debug(f"Smart limit for {leg['key']}: LTP={ltp:.2f}, Bid={bid:.2f}, Ask={ask:.2f} → Limit={limit_price:.2f}")
        return limit_price
    def _execute_leg_atomic(self, leg):
        limit_price = self._smart_limit_price(leg)
        expected_price = leg['ltp']
        logger.info(f"PLACING {leg['side']} {leg['strike']} {leg['type']} @ {limit_price} (Role: {leg['role']})")
        order_id = self.place_order(leg['key'], leg['qty'], leg['side'], "LIMIT", limit_price)
        if not order_id: return None
        start = time.time(); last_status = None
        while (time.time() - start) < ProductionConfig.ORDER_TIMEOUT:
            status = self.get_order_status(order_id)
            if not status: time.sleep(0.2); continue
            if status['status'] != last_status: logger.debug(f"Order {order_id}: {status['status']}"); last_status = status['status']
            if status['status'] == 'complete':
                fill_threshold = ProductionConfig.HEDGE_FILL_TOLERANCE if leg['role'] == 'HEDGE' else ProductionConfig.PARTIAL_FILL_TOLERANCE
                if status['filled_qty'] < leg['qty'] * fill_threshold:
                    logger.critical(f"PARTIAL FILL: {status['filled_qty']}/{leg['qty']} for {leg['role']}")
                    self.cancel_order(order_id)
                    db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "PARTIAL_REJECTED", filled_qty=status['filled_qty'], message=f"Below {fill_threshold*100:.0f}% threshold")
                    return None
                actual_price = status['avg_price']
                slippage = abs(actual_price - expected_price) / expected_price if expected_price > 0 else 0
                if slippage > ProductionConfig.SLIPPAGE_TOLERANCE:
                    logger.warning(f"SLIPPAGE: {slippage*100:.2f}% on {leg['key']}")
                    circuit_breaker.record_slippage_event(slippage)
                leg['entry_price'] = actual_price; leg['filled_qty'] = status['filled_qty']; leg['slippage'] = slippage
                db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "FILLED", filled_qty=status['filled_qty'], avg_price=actual_price)
                record_order_fill(leg, time.time() - start); record_slippage(leg)
                logger.info(f"✅ FILLED: {leg['side']} {status['filled_qty']}x {leg['strike']} {leg['type']} @ {actual_price}")
                return leg
            elif status['status'] in ['rejected', 'cancelled']:
                logger.error(f"ORDER DEAD: {status['status']}")
                db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, status['status'].upper())
                return None
            time.sleep(0.2)
        logger.warning(f"TIMEOUT on {order_id}. Attempting cancel...")
        self.cancel_order(order_id)
        time.sleep(1)
        final_status = self.get_order_status(order_id)
        if final_status and final_status['status'] == 'complete':
            leg['entry_price'] = final_status['avg_price']; leg['filled_qty'] = final_status['filled_qty']
            logger.info(f"Order filled during cancel: {final_status}")
            return leg
        order_timeout_counter.labels(side=leg['side'], role=leg['role']).inc()
        db_writer.log_order(order_id, leg['key'], leg['side'], leg['qty'], limit_price, "TIMEOUT")
        return None
    def execute_strategy(self, legs: List[Dict]) -> List[Dict]:
        total_qty = sum(l['qty'] for l in legs)
        if total_qty > ProductionConfig.MAX_CONTRACTS_PER_INSTRUMENT:
            logger.critical(f"Position size {total_qty} exceeds limit {ProductionConfig.MAX_CONTRACTS_PER_INSTRUMENT}")
            telegram.send(f"Position size violation: {total_qty} contracts", "ERROR")
            return []
        if len(legs) >= 4:
            strikes = sorted([l['strike'] for l in legs])
            max_spread_width = max(strikes) - min(strikes) if len(strikes) >= 2 else 0
            premium = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
            max_loss = (max_spread_width - premium) * legs[0]['qty']
            if max_loss > ProductionConfig.MAX_LOSS_PER_TRADE:
                logger.critical(f"Max loss ₹{max_loss:,.0f} exceeds limit ₹{ProductionConfig.MAX_LOSS_PER_TRADE:,.0f}")
                telegram.send(f"Max loss violation: ₹{max_loss:,.0f}", "ERROR")
                return []
        if not ProductionConfig.DRY_RUN_MODE:
            required_margin = self.check_margin_requirement(legs)
            available_funds = self.get_funds()
            usable_funds = available_funds * (1 - ProductionConfig.MARGIN_BUFFER)
            if required_margin > usable_funds:
                logger.critical(f"Margin ERROR: Need ₹{required_margin:,.2f}, Have ₹{usable_funds:,.2f} (with buffer)")
                telegram.send(f"Margin Shortfall: Need {required_margin/100000:.2f}L, Have {usable_funds/100000:.2f}L", "ERROR")
                return []
            projected_premium = sum(l['ltp'] * l['qty'] for l in legs if l['side'] == 'SELL')
            brokerage_cost = self.get_brokerage_impact(legs)
            if projected_premium > 0 and (projected_premium - brokerage_cost) < (projected_premium * 0.05):
                logger.critical(f"BROKERAGE TOO HIGH: Cost=₹{brokerage_cost:.2f}, Premium=₹{projected_premium:.2f}")
                telegram.send(f"Brokerage kills profit: ₹{brokerage_cost:.2f} on ₹{projected_premium:.2f} premium", "ERROR")
                return []
            update_margin_pct(required_margin, available_funds)
        else: logger.info("📄 Dry run - skipping margin and brokerage checks")
        hedges = [l for l in legs if l['role'] == 'HEDGE']
        cores = [l for l in legs if l['role'] == 'CORE']
        logger.info(f"📋 Execution Plan: {len(hedges)} Hedges → {len(cores)} Cores {'[DRY RUN]' if ProductionConfig.DRY_RUN_MODE else ''}")
        hedge_results = []
        if hedges:
            logger.info(f"Executing {len(hedges)} Hedges in Parallel...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(hedges), thread_name_prefix="Hedge-Exec") as executor:
                future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in hedges}
                for future in concurrent.futures.as_completed(future_to_leg):
                    result = future.result()
                    if result: hedge_results.append(result)
                    else:
                        logger.critical("HEDGE EXECUTION FAILED - ABORTING STRATEGY")
                        if hedge_results:
                            logger.warning(f"Flattening {len(hedge_results)} filled hedges")
                            self._emergency_flatten_hedges(hedge_results)
                        return []
            if len(hedge_results) != len(hedges):
                logger.critical(f"INCOMPLETE HEDGES: {len(hedge_results)}/{len(hedges)} - ABORTING")
                success = self._emergency_flatten_hedges(hedge_results)
                if not success:
                    telegram.send("💀 Emergency hedge cleanup failed - check account", "CRITICAL")
                return []
        logger.info(f"✅ All {len(hedge_results)} Hedges Filled Successfully")
        core_results = []
        if cores:
            logger.info(f"Executing {len(cores)} Cores in Parallel...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(cores), thread_name_prefix="Core-Exec") as executor:
                future_to_leg = {executor.submit(self._execute_leg_atomic, leg): leg for leg in cores}
                for future in concurrent.futures.as_completed(future_to_leg):
                    result = future.result()
                    if result: core_results.append(result)
                    else:
                        logger.critical("CORE EXECUTION FAILED - FLATTENING ALL")
                        self._emergency_flatten_hedges(hedge_results + core_results)
                        return []
            if len(core_results) != len(cores):
                logger.critical(f"INCOMPLETE CORES: {len(core_results)}/{len(cores)} - FLATTENING ALL")
                success = self._emergency_flatten_hedges(hedge_results + core_results)
                if not success:
                    telegram.send("💀 Emergency core cleanup failed - check account", "CRITICAL")
                return []
        executed = hedge_results + core_results
        structure = executed[0].get('structure', 'UNKNOWN') if executed else 'UNKNOWN'
        actual_premium = sum(l['entry_price'] * l['filled_qty'] for l in executed if l['side'] == 'SELL')
        actual_debit = sum(l['entry_price'] * l['filled_qty'] for l in executed if l['side'] == 'BUY')
        net_premium = actual_premium - actual_debit
        db_writer.update_daily_stats(trades=1)
        mode_indicator = "📄 PAPER" if ProductionConfig.DRY_RUN_MODE else "💰 LIVE"
        logger.info(f"✅ {mode_indicator} STRATEGY DEPLOYED: {structure} | Net Premium: ₹{net_premium:,.2f}")
        telegram.send(f"{mode_indicator} Position Opened\nStructure: {structure}\nLegs: {len(executed)}\nNet Premium: ₹{net_premium:,.2f}", "TRADE")
        return executed
    def _emergency_flatten_hedges(self, filled_hedges: List[Dict]) -> bool:
        logger.critical(f"🚨 EMERGENCY HEDGE FLATTEN: {len(filled_hedges)} positions")
        telegram.send(f"🔴 **CRITICAL ALERT**\nEmergency hedge exit initiated\nPositions: {len(filled_hedges)}", "CRITICAL")
        try:
            logger.info("Attempting server-side atomic exit...")
            order_api = OrderApi(self.api_client)
            response = order_api.exit_positions()
            if response.status == 'success':
                logger.info("✅ Server-side atomic exit successful")
                telegram.send("✅ Emergency exit completed (server-side)", "SUCCESS")
                return True
        except Exception as e:
            logger.error(f"Atomic exit failed: {e}")
        logger.warning("⚠️  Falling back to MARKET order cleanup...")
        all_cleared = True
        for leg in filled_hedges:
            try:
                exit_side = 'SELL' if leg['side'] == 'BUY' else 'BUY'
                logger.info(f"Placing MARKET order: {exit_side} {leg['filled_qty']}x {leg['key']}")
                order_id = self.place_order(leg['key'], leg['filled_qty'], exit_side, "MARKET", 0.0)
                if order_id:
                    time.sleep(0.5)
                    status = self.get_order_status(order_id)
                    if status and status['status'] == 'complete':
                        logger.info(f"✅ Emergency MARKET exit: {leg['key']} @ {status['avg_price']}")
                    else:
                        logger.error(f"MARKET order did not fill immediately: {leg['key']}")
                        all_cleared = False
                else:
                    logger.critical(f"Failed to place MARKET order for {leg['key']}")
                    all_cleared = False
            except Exception as e:
                logger.critical(f"💀 FAILED TO EXIT {leg['key']}: {e}")
                all_cleared = False
                db_writer.log_risk_event("FAILED_EMERGENCY_EXIT", "CRITICAL", f"Could not close {leg['key']} - position may be LIVE", "MANUAL_INTERVENTION_REQUIRED")
        if not all_cleared:
            telegram.send(f"💀 **MANUAL ACTION REQUIRED**\n\nFailed to close {len([l for l in filled_hedges if not all_cleared])} positions\n\n🔴 CALL BROKER IMMEDIATELY:\nUpstox Support: 022-6130-8000\n\nPositions may still be open in your account.", "CRITICAL")
        return all_cleared
    def _flatten_legs(self, legs: List[Dict]):
        if not legs: return
        logger.critical(f"🚨 EMERGENCY FLATTEN: {len(legs)} legs")
        telegram.send(f"Emergency flattening {len(legs)} legs", "CRITICAL")
        for leg in legs:
            if leg.get('filled_qty', 0) <= 0: continue
            exit_side = 'SELL' if leg['side'] == 'BUY' else 'BUY'
            success = False
            for attempt in range(2):
                try:
                    oid = self.place_order(leg['key'], leg['filled_qty'], exit_side, "MARKET", 0.0)
                    if oid:
                        time.sleep(1)
                        status = self.get_order_status(oid)
                        if status and status['status'] == 'complete':
                            logger.info(f"✅ Market exit: {leg['key']}")
                            success = True; break
                except Exception as e: logger.error(f"Market exit failed: {e}")
            if success: continue
            for attempt in range(3):
                try:
                    exit_price = leg.get('current_ltp', leg['entry_price'])
                    exit_price = exit_price * 1.10 if exit_side == 'BUY' else exit_price * 0.90
                    oid = self.place_order(leg['key'], leg['filled_qty'], exit_side, "LIMIT", round(exit_price, 1))
                    if oid:
                        time.sleep(2)
                        status = self.get_order_status(oid)
                        if status and status['status'] == 'complete':
                            logger.info(f"✅ Limit exit: {leg['key']}")
                            success = True; break
                        elif status and status['status'] != 'complete':
                            self.cancel_order(oid)
                except Exception as e: logger.error(f"Limit exit attempt {attempt+1} failed: {e}")
                time.sleep(1)
            if not success:
                msg = f"❌ CRITICAL: FAILED TO CLOSE {leg['key']} - MANUAL INTERVENTION REQUIRED"
                logger.critical(msg); telegram.send(msg, "CRITICAL")
                db_writer.log_risk_event("FAILED_EXIT", "CRITICAL", f"Could not close {leg['key']}", "MANUAL_ACTION_REQUIRED")

# ----------  INSTRUMENT VALIDATOR  ----------
class InstrumentValidator:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.ban_list_cache = set(); self.cache_time = 0; self.cache_ttl = 3600
    def is_instrument_banned(self, instrument_key: str) -> bool:
        if ProductionConfig.DRY_RUN_MODE: return False
        try:
            if time.time() - self.cache_time > self.cache_ttl: self._refresh_ban_list()
            return instrument_key in self.ban_list_cache
        except Exception as e: logger.error(f"Ban list check failed: {e}"); return False
    def _refresh_ban_list(self):
        try:
            url = "https://www.nseindia.com/api/fo-ban-securities"
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.ban_list_cache = set(data.get('data', []))
                self.cache_time = time.time()
                logger.info(f"Ban list refreshed: {len(self.ban_list_cache)} instruments")
        except Exception as e: logger.warning(f"Failed to refresh ban list: {e}")
    def validate_price(self, current_price: float, previous_price: float) -> bool:
        if previous_price <= 0: return True
        change_pct = abs(current_price - previous_price) / previous_price
        if change_pct > ProductionConfig.PRICE_CHANGE_THRESHOLD:
            logger.error(f"Price changed {change_pct*100:.1f}% - exceeds threshold")
            return False
        return True
    def validate_contract_exists(self, instrument_key: str) -> bool:
        if ProductionConfig.DRY_RUN_MODE: return True
        if not api_rate_limiter.acquire(): return False
        try:
            market_api = upstox_client.MarketQuoteV3Api(self.api_client)
            response = market_api.get_ltp(instrument_key=instrument_key)
            return response.status == 'success' and response.data
        except Exception as e: logger.error(f"Contract validation failed: {e}"); return False
    def validate_lot_size(self, instrument_key: str, expected_lot_size: int) -> bool:
        if ProductionConfig.DRY_RUN_MODE: return True
        if not api_rate_limiter.acquire(): return False
        try:
            options_api = OptionsApi(self.api_client)
            response = options_api.get_option_contracts(instrument_key=instrument_key)
            if response.status == 'success' and response.data:
                actual_lot_size = next((int(c.lot_size) for c in response.data if hasattr(c, 'lot_size')), 0)
                if actual_lot_size != expected_lot_size:
                    logger.error(f"Lot size mismatch: Expected {expected_lot_size}, Got {actual_lot_size}")
                    telegram.send(f"⚠️ Lot size changed: {expected_lot_size} → {actual_lot_size}", "WARNING")
                    return False
            return True
        except Exception as e: logger.error(f"Lot size validation failed: {e}"); return True

# ----------  RISK MANAGER  ----------
class RiskManager:
    def __init__(self, api_client: upstox_client.ApiClient, legs: List[Dict], expiry_date: date, trade_id: str, gtt_ids: List[str] = None):
        self.api_client = api_client; self.legs = legs; self.expiry = expiry_date; self.trade_id = trade_id
        self.gtt_ids = gtt_ids or []; self.running = True; self.last_price_update = time.time()
        credit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'SELL')
        debit = sum(l['entry_price'] * l['filled_qty'] for l in legs if l['side'] == 'BUY')
        self.net_premium = credit - debit
        strikes = sorted([l['strike'] for l in legs])
        max_spread_width = max(strikes) - min(strikes) if len(strikes) >= 2 else 0
        total_width_value = max_spread_width * legs[0]['filled_qty'] if legs else 0
        self.max_spread_loss = max(0, total_width_value - self.net_premium)
        logger.info(f"Risk Manager Init: Trade={trade_id} | Premium=₹{self.net_premium:,.2f} | Max Risk=₹{self.max_spread_loss:,.2f} | GTTs={len(self.gtt_ids)}")
        # NEW: WebSocket Greeks
        self.greeks_monitor = GreeksMonitor(self.api_client, self.legs)

    # SMART EXPIRY EXIT (Expiry-Minus-1 Day)
    def monitor(self):
        market_api = upstox_client.MarketQuoteV3Api(self.api_client); consecutive_errors = 0; max_consecutive_errors = 10
        logger.info(f"🔍 Risk monitoring started for {self.trade_id}")
        while self.running:
            try:
                now_ist = datetime.now(ProductionConfig.IST)
                today = now_ist.date()
                current_time = now_ist.time()
                days_to_expiry = (self.expiry - today).days

                exit_start = dtime(14, 15)   # 2:15 PM
                exit_end   = dtime(15, 15)   # 3:15 PM

                # 1. EMERGENCY: Position open on Expiry Day (0 DTE)
                if days_to_expiry <= 0:
                    logger.critical(f"🚨 EXPIRY DAY DETECTED (0 DTE) - EMERGENCY EXIT")
                    telegram.send(f"🚨 EXPIRY DAY EMERGENCY — Flattening {self.trade_id}", "CRITICAL")
                    self.flatten_all("EXPIRY_DAY_EMERGENCY"); return

                # 2. SMART EXIT: Expiry-Minus-1 (1 DTE)
                elif days_to_expiry == 1:
                    if exit_start <= current_time <= exit_end:
                        logger.info(f"✅ Smart Expiry Exit triggered at {current_time.strftime('%H:%M')}")
                        telegram.send(f"📅 Smart Expiry Exit triggered — {self.trade_id}", "TRADE")
                        self.flatten_all("SMART_EXPIRY_EXIT"); return
                    elif current_time > exit_end:
                        logger.warning(f"⚠️  Late Expiry Exit (Past {exit_end}) — Forcing exit")
                        self.flatten_all("EXPIRY_MINUS_1_LATE_EXIT"); return
                    elif now_ist.minute % 30 == 0 and now_ist.second < 5:
                        mins_to_go = (datetime.combine(today, exit_start) - now_ist).seconds // 60
                        logger.info(f"⏳ Holding for Smart Exit. Window opens in {mins_to_go} mins ({exit_start})")

                keys = [l['key'] for l in self.legs]
                price_response = None
                for attempt in range(3):
                    try:
                        price_response = market_api.get_ltp(instrument_key=','.join(keys))
                        if price_response and price_response.status == 'success': break
                    except Exception as e:
                        logger.warning(f"Price fetch attempt {attempt+1} failed: {e}")
                        time.sleep(0.5)
                if not price_response or price_response.status != 'success':
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.critical(f"Price feed failed {consecutive_errors} times - flattening for safety")
                        self.flatten_all("PRICE_FEED_FAILURE"); return
                    time.sleep(ProductionConfig.POLL_INTERVAL); continue
                consecutive_errors = 0; prices = price_response.data; self.last_price_update = time.time()
                if time.time() - self.last_price_update > ProductionConfig.PRICE_STALENESS_THRESHOLD: logger.warning("Price data is stale")
                price_staleness_sec.set(time.time() - self.last_price_update)
                current_pnl = self._calculate_pnl(prices)
                if self.max_spread_loss > 0 and current_pnl < -(self.max_spread_loss * 0.80):
                    logger.critical(f"Max risk breached: P&L={current_pnl:.2f}, Limit={self.max_spread_loss:.2f}")
                    self.flatten_all("STOP_LOSS_MAX_RISK"); return
                stop_threshold = -(self.net_premium * ProductionConfig.STOP_LOSS_PCT)
                if self.net_premium > 0 and current_pnl < stop_threshold - 0.1:
                    logger.critical(f"Stop loss hit: P&L={current_pnl:.2f}, Threshold={stop_threshold:.2f}")
                    self.flatten_all("STOP_LOSS_PREMIUM"); return
                target_pnl = self.net_premium * ProductionConfig.TARGET_PROFIT_PCT
                if self.net_premium > 0 and (current_pnl >= target_pnl - 0.1):
                    logger.info(f"Target profit reached: P&L={current_pnl:.2f}, Target={target_pnl:.2f}")
                    self.flatten_all("TARGET_PROFIT"); return
                self._update_dashboard_state(current_pnl)
                greeks = self.greeks_monitor.calculate_portfolio_greeks(prices)
                warnings = self.greeks_monitor.check_greek_limits(greeks)
                if warnings:
                    for w in warnings: logger.warning(w)
                    telegram.send("\n".join(warnings), "WARNING")
                time.sleep(ProductionConfig.POLL_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Risk monitor interrupted by user"); self.running = False; return
            except Exception as e:
                logger.error(f"Risk monitor error: {e}"); traceback.print_exc(); consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("Too many errors in risk monitor - emergency exit")
                    self.flatten_all("MONITOR_ERROR"); return
                time.sleep(5)
    def _calculate_pnl(self, prices) -> float:
        pnl = 0.0; missing_data_count = 0
        for leg in self.legs:
            current_price = 0.0
            if leg['key'] in prices and hasattr(prices[leg['key']], 'last_price'):
                current_price = prices[leg['key']].last_price
                leg['last_known_ltp'] = current_price
            elif```python
                'last_known_ltp' in leg:
                current_price = leg['last_known_ltp']
                logger.warning(f"⚠️  Using stale price for {leg['key']}: {current_price}")
            else:
                missing_data_count += 1
                logger.error(f"❌ NO DATA for {leg['key']}")
                continue
            if leg['side'] == 'SELL':
                leg_pnl = (leg['entry_price'] - current_price) * leg['filled_qty']
            else:
                leg_pnl = (current_price - leg['entry_price']) * leg['filled_qty']
            pnl += leg_pnl
        if missing_data_count > 0:
            logger.critical(f"Incomplete P&L calc due to missing data on {missing_data_count} legs")
            if missing_data_count > len(self.legs) / 2:
                self.flatten_all("DATA_FEED_LOSS")
        return pnl
    def _update_dashboard_state(self, current_pnl: float):
        try:
            pnl_pct = (current_pnl / self.net_premium * 100) if self.net_premium > 0 else 0
            trade_pnl.labels(trade_id=self.trade_id, strategy="LIVE").set(current_pnl)
        except: pass
    def flatten_all(self, reason="SIGNAL"):
        logger.critical(f"🚨 FLATTEN TRIGGERED: {reason}")
        telegram.send(f"🚨 Position Exit: {reason}", "CRITICAL")
        # Kill GTTs first
        if self.gtt_ids:
            logger.info(f"Cancelling {len(self.gtt_ids)} GTT orders...")
            for gtt_id in self.gtt_ids:
                cancelled = False
                for _ in range(3):
                    if self.cancel_gtt_order(gtt_id): cancelled = True; break
                    time.sleep(0.5)
                if not cancelled:
                    msg = f"❌ FATAL: Could not cancel GTT {gtt_id}. Manual intervention required."
                    logger.critical(msg); telegram.send(msg, "CRITICAL")
        # Exit positions
        executor = ExecutionEngine(self.api_client)
        atomic_success = False
        for attempt in range(2):
            logger.info(f"Atomic exit attempt {attempt+1}...")
            if executor.exit_all_positions(tag="VG30"): atomic_success = True; break
            time.sleep(2)
        if not atomic_success:
            logger.critical("Atomic exit failed - falling back to leg-by-leg")
            telegram.send("Atomic exit failed - manual closure initiated", "CRITICAL")
            executor._flatten_legs(self.legs)
        # Reporting
        final_pnl = self._get_final_pnl()
        db_writer.update_trade_exit(self.trade_id, reason, final_pnl)
        db_writer.log_risk_event("POSITION_EXIT", "INFO", reason, f"P&L: ₹{final_pnl:.2f}")
        circuit_breaker.record_trade_result(final_pnl)
        telegram.send(f"Position Closed\nReason: {reason}\nFinal P&L: ₹{final_pnl:,.2f}", "SUCCESS" if final_pnl > 0 else "WARNING")
        self.running = False
        logger.info(f"Risk monitor shutdown complete for {self.trade_id}")
        # NEW: Stop Greeks WebSocket to prevent connection leak
        if hasattr(self, 'greeks_monitor'):
            self.greeks_monitor.stop()
    def _get_final_pnl(self) -> float:
        try:
            portfolio_api = PortfolioApi(self.api_client)
            response = portfolio_api.get_positions(api_version="2.0")
            if response.status == 'success' and response.data:
                return sum(float(p.pnl) for p in response.data)
            return 0.0
        except: return 0.0

# ----------  SESSION MANAGER  ----------
class SessionManager:
    def __init__(self, api_client: upstox_client.ApiClient):
        self.api_client = api_client
        self.last_validation = 0
        self.validation_interval = 3600
    def validate_session(self, force: bool = False) -> bool:
        if not force and (time.time() - self.last_validation) < self.validation_interval: return True
        try:
            user_api = upstox_client.UserApi(self.api_client)
            response = user_api.get_profile(api_version="2.0")
            if response.status == 'success':
                self.last_validation = time.time(); logger.debug("Session validated successfully"); return True
            else:
                logger.warning("Session invalid - attempting refresh")
                return self._refresh_token()
        except Exception as e:
            logger.error(f"Session validation error: {e}")
            return self._refresh_token()
    def _refresh_token(self) -> bool:
        logger.critical("⚠️  UPSTOX TOKEN EXPIRED - Requesting new authorization")
        telegram.send("🔴 *SESSION EXPIRED*\n\nUpstox access token has expired.\nStarting authentication flow...", "CRITICAL")
        success = token_manager.request_token_via_telegram()
        if success:
            new_token = ProductionConfig.UPSTOX_ACCESS_TOKEN
            self.api_client.configuration.access_token = new_token
            self.last_validation = time.time()
            logger.info("✅ Session refreshed with new token")
            return True
        else:
            logger.critical("❌ Failed to obtain new token - HALTING SYSTEM")
            telegram.send("🚨 *AUTHENTICATION FAILED*\n\nCould not obtain new Upstox token.\nSystem will halt to prevent unauthorized trading.", "CRITICAL")
            return False
    def check_market_status(self) -> bool:
        try:
            market_api = MarketHolidaysAndTimingsApi(self.api_client)
            status_response = market_api.get_market_status(exchange='NFO')
            if status_response.status == 'success' and status_response.data:
                market_status = status_response.data.status.upper() if hasattr(status_response.data, 'status') else 'UNKNOWN'
                if market_status == 'OPEN': return True
                logger.info(f"Market status: {market_status}")
            today_str = date.today().strftime("%Y-%m-%d")
            holiday_response = market_api.get_holiday(today_str)
            if holiday_response.status == 'success' and holiday_response.data:
                holidays = holiday_response.data if isinstance(holiday_response.data, list) else [holiday_response.data]
                for holiday in holidays:
                    if hasattr(holiday, 'holiday_type') and holiday.holiday_type == 'TRADING_HOLIDAY':
                        logger.info(f"Market Closed: {getattr(holiday, 'description', 'Holiday')}")
                        return False
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 15:
                logger.warning("Could not determine market status - assuming open during market hours")
                return True
            return False
        except Exception as e:
            logger.error(f"Market status check failed: {e}")
            current_hour = datetime.now().hour
            return 9 <= current_hour <= 15

# ----------  AI ANALYST  ----------
@dataclass
class MarketRegime:
    risk_score: int
    nifty_view: str
    strategy: str
    reasoning: str
class NewsScout:
    def __init__(self):
        self.rss_url = "https://news.google.com/rss/search?q=stock+market+india+business+sensex+nifty&hl=en-IN&gl=IN&ceid=IN:en"
    def get_headlines(self, limit=5):
        try:
            response = requests.get(self.rss_url, timeout=5)
            if response.status_code != 200: return []
            root = ET.fromstring(response.content)
            headlines = []
            for item in root.findall('./channel/item')[:limit]:
                title = item.find('title').text
                if "-" in title: title = title.rpartition('-')[0].strip()
                headlines.append(title)
            return headlines
        except: return []
class EventRadar:
    def __init__(self):
        self.url = "https://economic-calendar.tradingview.com/events"
        self.headers = {"User-Agent": "Mozilla/5.0", "Origin": "https://in.tradingview.com"}
    def get_todays_events(self):
        try:
            now_utc = datetime.now(pytz.utc)
            payload = {"from": now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "to": (now_utc + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z"), "countries": "IN,US", "minImportance": "1"}
            r = requests.get(self.url, params=payload, headers=self.headers, timeout=5)
            events = []
            for e in r.json().get('result', []):
                if e.get('importance', 0) > 0:
                    dt_utc = datetime.strptime(e['date'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                    dt_ist = dt_utc.astimezone(ProductionConfig.IST)
                    events.append(f"[{e['country']}] {e['title']} @ {dt_ist.strftime('%I:%M %p')} IST")
            return events[:7]
        except: return []
def get_market_metrics():
    tickers = {"^NSEI": "Nifty 50", "^INDIAVIX": "India VIX", "ES=F": "US Futures", "GC=F": "Gold"}
    try:
        data = yf.download(list(tickers.keys()), period="5d", progress=False)['Close']
        metrics = {}
        for symbol, name in tickers.items():
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if symbol in data.columns.levels[1]: s = data.xs(symbol, axis=1, level=1).dropna()
                    else: s = pd.Series()
                else: s = data[symbol].dropna()
                if s.empty and symbol in data: s = data[symbol].dropna()
                if len(s) >= 2:
                    metrics[name] = f"{((s.iloc[-1] - s.iloc[-2]) / s.iloc[-2]) * 100:+.2f}%"
                else: metrics[name] = "0.00%"
            except: metrics[name] = "Err"
        return metrics
    except: return {}
class IndiaAIAnalyst:
    def __init__(self, api_key):
        self.client = Groq(api_key=api_key)
        self.model = "llama-3.3-70b-versatile"
        self.scout = NewsScout()
        self.radar = EventRadar()
    def get_morning_brief(self) -> MarketRegime:
        news = self.scout.get_headlines()
        events = self.radar.get_todays_events()
        data = get_market_metrics()
        system = "You are a Nifty 50 Risk Officer. Output JSON only."
        prompt = f"""
        Analyze Indian Market: {datetime.now(ProductionConfig.IST).strftime('%d %b %Y')}.
        DATA: {json.dumps(data)}
        EVENTS: {json.dumps(events)}
        NEWS: {json.dumps(news)}
        RISK RULES:
        - War/Terror/Sanctions + Gold UP = FEAR (Risk 8-10).
        - VIX > +5% = VOLATILITY (Risk 6-7).
        - Fed/RBI/CPI/Election = EVENT_RISK.
        OUTPUT JSON:
        {{
            "risk_score": (int 1-10),
            "nifty_view": "GAP_UP"|"GAP_DOWN"|"FLAT",
            "strategy": "IRON_CONDOR"|"CREDIT_SPREAD"|"CASH_ONLY",
            "reason": "Brief summary."
        }}
        """
        try:
            chat = self.client.chat.completions.create(messages=[{"role":"system","content":system}, {"role":"user","content":prompt}], model=self.model, response_format={"type": "json_object"})
            res = json.loads(chat.choices[0].message.content)
            return MarketRegime(risk_score=res.get('risk_score', 5), nifty_view=res.get('nifty_view', 'FLAT'), strategy=res.get('strategy', 'IRON_CONDOR'), reasoning=res.get('reason', 'AI Analysis'))
        except Exception as e:
            return MarketRegime(5, "FLAT", "IRON_CONDOR", f"AI Error: {str(e)}")

# ----------  SHADOW ENGINE  ----------
@dataclass
class ShadowLeg:
    key: str
    strike: float
    type: str
    side: str
    qty: int
    expected_price: float
    expected_slippage: float
    expected_fill_time: float
@dataclass
class ShadowTrade:
    trade_id: str
    mandate: Dict
    legs: List[ShadowLeg]
    timestamp: str
    live_spot: float
    live_vix: float
    expected_net_premium: float
    expected_max_risk: float
class ShadowEngine:
    def __init__(self):
        self.trades: Dict[str, ShadowTrade] = {}
        self.running = True
        self.thread = threading.Thread(target=self._mark_to_market, daemon=True, name="Shadow-MTM")
        self.thread.start()
        logger.info("🎭 Shadow Engine (Paper Trading) Started")
    def execute_strategy(self, legs: List[Dict]) -> List[Dict]:
        shadow_legs = []; net_premium = 0.0
        for leg in legs:
            slippage = self._estimate_slippage(leg)
            fill_price = leg["ltp"] * (1 + slippage if leg["side"] == "BUY" else 1 - slippage)
            shadow_legs.append(ShadowLeg(
                key=leg["key"], strike=leg["strike"], type=leg["type"], side=leg["side"], qty=leg["qty"],
                expected_price=round(fill_price, 2), expected_slippage=round(slippage * 100, 2), expected_fill_time=time.time()
            ))
            premium = (fill_price * leg["qty"]) * (-1 if leg["side"] == "SELL" else 1)
            net_premium += premium
        trade_id = f"SHADOW_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        live_spot = getattr(ProductionConfig, 'last_known_spot', 0.0)
        live_vix = getattr(ProductionConfig, 'last_known_vix', 0.0)
        shadow_trade = ShadowTrade(
            trade_id=trade_id, mandate=legs[0].get("mandate_snapshot", {}), legs=shadow_legs,
            timestamp=datetime.now(ProductionConfig.IST).isoformat(), live_spot=live_spot, live_vix=live_vix,
            expected_net_premium=round(net_premium, 2), expected_max_risk=self._expected_max_risk(shadow_legs)
        )
        self.trades[trade_id] = shadow_trade
        self._log_shadow_fill(shadow_trade)
        return self._to_live_format(shadow_legs, trade_id)
    def _mark_to_market(self):
        while self.running:
            try:
                for trade in self.trades.values():
                    mtm_pnl = 0.0
                    for leg in trade.legs:
                        ltp = leg.expected_price 
                        unrealised = (ltp - leg.expected_price) * leg.qty * (-1 if leg.side == "SELL" else 1)
                        mtm_pnl += unrealised
                    try: trade_pnl.labels(trade_id=trade.trade_id, strategy="SHADOW").set(mtm_pnl)
                    except: pass
                time.sleep(30)
            except Exception as e:
                logger.error(f"Shadow MTM Error: {e}")
                time.sleep(30)
    @staticmethod
    def _estimate_slippage(leg: Dict) -> float:
        base = 0.001; role_adj = 0.0005 if leg.get("role") == "HEDGE" else 0.001
        try: spread = (leg.get("ask", 0) - leg.get("bid", 0)) / leg["ltp"] if leg.get("ltp") > 0 else 0
        except: spread = 0.01
        spread_adj = max(0, spread); noise = np.random.normal(0, 0.0005)
        return base + role_adj + spread_adj + noise
    @staticmethod
    def _expected_max_risk(shadow_legs: List[ShadowLeg]) -> float:
        strikes = [l.strike for l in shadow_legs]
        if not strikes: return 0.0
        width = max(strikes) - min(strikes) if len(strikes) >= 2 else 0
        qty = shadow_legs[0].qty if shadow_legs else 0
        credit = sum(l.expected_price * l.qty for l in shadow_legs if l.side == "SELL")
        debit = sum(l.expected_price * l.qty for l in shadow_legs if l.side == "BUY")
        return max(0, (width * qty) - (credit - debit))
    def _log_shadow_fill(self, trade: ShadowTrade):
        db_writer.log_order(order_id=trade.trade_id, instrument_key="SHADOW_BASKET", side="SHADOW", qty=sum(l.qty for l in trade.legs),
                           price=trade.expected_net_premium, status="SHADOW_FILLED", message=f"Risk: {trade.expected_max_risk:.0f}")
        telegram.send(f"🎭 **SHADOW TRADE EXECUTED**\nID: `{trade.trade_id}`\nPremium: ₹{trade.expected_net_premium:,.0f}\nRisk: ₹{trade.expected_max_risk:,.0f}\nSpot: {trade.live_spot:.2f} | VIX: {trade.live_vix:.2f}", "INFO")
    @staticmethod
    def _to_live_format(shadow_legs: List[ShadowLeg], trade_id: str) -> List[Dict]:
        return [{**leg.__dict__, "entry_price": leg.expected_price, "filled_qty": leg.qty, "slippage": leg.expected_slippage / 100,
                 "structure": "SHADOW", "role": "CORE" if leg.side == "SELL" else "HEDGE", "trade_id": trade_id, "ltp": leg.expected_price, "last_known_ltp": leg.expected_price} for leg in shadow_legs]
    def shutdown(self):
        self.running = False
        try: self.thread.join(timeout=2)
        except: pass
        logger.info("🎭 Shadow Engine Stopped")

# ----------  TRADING ORCHESTRATOR  ----------
class TradingOrchestrator:
    def __init__(self):
        self.configuration = upstox_client.Configuration()
        self.configuration.access_token = ProductionConfig.UPSTOX_ACCESS_TOKEN
        self.api_client = upstox_client.ApiClient(self.configuration)
        self.analytics_queue = multiprocessing.Queue()
        self.analytics_process = None
        self.regime_engine = RegimeEngine()
        self.strategy_factory = StrategyFactory(self.api_client)
        self.execution_engine = ExecutionEngine(self.api_client)
        self.shadow_engine = ShadowEngine()
        self.session_manager = SessionManager(self.api_client)
        self.last_analysis = None
        self.current_trade_id = None
        self.current_risk_manager = None
        self.market_api = MarketHolidaysAndTimingsApi(self.api_client)
        self.cached_holiday_date = None
        self.is_holiday_today = False
        self.last_brief_date = None
        self.last_analysis_time = None
        # NEW: Multi-trade recovery list
        self.active_risk_managers = []
        atexit.register(self._cleanup_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _cleanup_handler(self):
        logger.info("Cleanup handler triggered")
        heartbeat.stop()
        try:
            if self.analytics_process and self.analytics_process.is_alive():
                self.analytics_process.terminate()
                self.analytics_process.join(timeout=5)
                if self.analytics_process.exitcode is None:
                    self.analytics_process.kill()
        except: pass
        try: self.shadow_engine.shutdown()
        except: pass
        db_writer.shutdown()

    def _signal_handler(self, signum, frame):
        logger.critical(f"Received signal {signum}")
        telegram.send(f"System shutdown signal received: {signum}", "CRITICAL")
        if self.current_risk_manager and self.current_risk_manager.running:
            logger.critical("Emergency position exit on shutdown")
            self.current_risk_manager.flatten_all("SYSTEM_SHUTDOWN")
        self._cleanup_handler()
        sys.exit(0)

    def check_market_open_status(self, current_date):
        if self.cached_holiday_date == current_date: return not self.is_holiday_today
        try:
            logger.info(f"📅 Checking Holiday API for {current_date}...")
            response = self.market_api.get_holiday(str(current_date))
            is_holiday = False
            if response.data:
                for h in response.data:
                    if h.holiday_type == "TRADING_HOLIDAY":
                        is_holiday = True
                        logger.info(f"💤 Market Closed Today: {h.description}")
                        break
            self.cached_holiday_date = current_date
            self.is_holiday_today = is_holiday
            return not is_holiday
        except Exception as e:
            logger.error(f"Holiday API Failed: {e}")
            KNOWN_HOLIDAYS = ["2025-01-26", "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-18"]
            if str(current_date) in KNOWN_HOLIDAYS: return False
            return current_date.weekday() < 5

    def run_morning_brief(self):
        today = datetime.now(ProductionConfig.IST).date()
        if self.last_brief_date == today: return
        try:
            saved = db_writer.get_state("daily_risk_regime")
            if saved and json.loads(saved).get("date") == str(today):
                logger.info("✅ Morning Brief already in DB.")
                self.last_brief_date = today
                return
        except: pass
        logger.info("🌅 INITIATING MORNING BRIEF...")
        self.last_brief_date = today
        if not ProductionConfig.GROQ_API_KEY: return
        try:
            brief = IndiaAIAnalyst(ProductionConfig.GROQ_API_KEY).get_morning_brief()
            emoji = "🟢" if brief.risk_score <= 4 else "🟡" if brief.risk_score <= 7 else "🔴"
            telegram.send(f"🌅 **MORNING BRIEF**\n{emoji} **Risk:** {brief.risk_score}/10\n📉 **View:** {brief.nifty_view}\n🛡️ **Plan:** {brief.strategy}\n\n💡 _{brief.reasoning}_", "INFO")
            db_writer.set_state("daily_risk_regime", json.dumps({"date": str(today), "risk_score": brief.risk_score, "reason": brief.reasoning}))
        except Exception as e: logger.error(f"Brief Failed: {e}")

    def run_analysis(self) -> Optional[Dict]:
        logger.info("Starting market analysis process...")
        try:
            if self.analytics_process and self.analytics_process.is_alive():
                logger.warning("Terminating existing analytics process")
                self.analytics_process.terminate()
                self.analytics_process.join(timeout=5)
                if self.analytics_process.exitcode is None:
                    self.analytics_process.kill()
            while not self.analytics_queue.empty():
                try: self.analytics_queue.get_nowait()
                except: break
            config = {'access_token': ProductionConfig.UPSTOX_ACCESS_TOKEN}
            self.analytics_process = multiprocessing.Process(target=AnalyticsEngine(self.analytics_queue).run, args=(config,), daemon=True, name="Analytics-Process")
            self.analytics_process.start()
            logger.info(f"Analytics process started: PID={self.analytics_process.pid}")
            status, result = self.analytics_queue.get(timeout=ProductionConfig.ANALYTICS_PROCESS_TIMEOUT)
            if status == 'success':
                weekly_mandate = self.regime_engine.generate_mandate(self.regime_engine.calculate_scores(result['vol_metrics'], result['struct_metrics_weekly'], result['edge_metrics'], result['external_metrics'], result['time_metrics'], "WEEKLY"),
                                                                     result['vol_metrics'], result['struct_metrics_weekly'], result['edge_metrics'], result['external_metrics'], result['time_metrics'], "WEEKLY", result['time_metrics'].weekly_exp, result['time_metrics'].dte_weekly)
                monthly_mandate = self.regime_engine.generate_mandate(self.regime_engine.calculate_scores(result['vol_metrics'], result['struct_metrics_monthly'], result['edge_metrics'], result['external_metrics'], result['time_metrics'], "MONTHLY"),
                                                                      result['vol_metrics'], result['struct_metrics_monthly'], result['edge_metrics'], result['external_metrics'], result['time_metrics'], "MONTHLY", result['time_metrics'].monthly_exp, result['time_metrics'].dte_monthly)
                self.last_analysis = {
                    'timestamp': datetime.now(), 'time_metrics': result['time_metrics'], 'vol_metrics': result['vol_metrics'],
                    'weekly_mandate': weekly_mandate, 'monthly_mandate': monthly_mandate,
                    'weekly_chain': result['weekly_chain'], 'monthly_chain': result['monthly_chain'], 'lot_size': result['lot_size']
                }
                ProductionConfig.last_known_spot = result['vol_metrics'].spot
                ProductionConfig.last_known_vix = result['vol_metrics'].vix
                logger.info(f"✅ Analysis Complete\nWeekly: {weekly_mandate.regime_name} | Score: {weekly_mandate.score.composite:.2f} | {weekly_mandate.suggested_structure}\nMonthly: {monthly_mandate.regime_name} | Score: {monthly_mandate.score.composite:.2f} | {monthly_mandate.suggested_structure}")
                return self.last_analysis
            else:
                logger.error(f"Analytics process failed: {result}")
                telegram.send(f"Analysis failed: {result}", "ERROR")
                return None
        except queue.Empty:
            logger.error("Analytics process timeout")
            telegram.send("Analysis timeout - process killed", "ERROR")
            if self.analytics_process.is_alive():
                self.analytics_process.terminate()
                self.analytics_process.join(timeout=2)
                if self.analytics_process.exitcode is None:
                    self.analytics_process.kill()
            return None
        except Exception as e:
            logger.error(f"Analysis error: {e}")
            traceback.print_exc()
            return None

    # NEW: Multi-trade recovery method
    def _recover_active_trades(self):
        logger.info("♻️  Checking for active trades to recover...")
        conn = db_writer._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT trade_id, legs_json, expiry_date FROM trades WHERE status = 'OPEN'")
            rows = cursor.fetchall()
            if not rows:
                logger.info("✅ No orphaned trades found")
                return
            logger.warning(f"⚠️  Found {len(rows)} orphaned trades - initiating recovery...")
            for row in rows:
                trade_id, legs_json, expiry_str = row
                try:
                    legs = json.loads(legs_json)
                    expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date() if isinstance(expiry_str, str) else expiry_str
                    logger.warning(f"⚠️  RECOVERING: {trade_id} (Expiry: {expiry_date})")
                    telegram.send(f"♻️ **System Restart Detected**\nResuming monitoring: `{trade_id}`\nExpiry: {expiry_date}\nLegs: {len(legs)}", "WARNING")
                    rm = RiskManager(self.api_client, legs, expiry_date, trade_id)
                    thread = threading.Thread(target=rm.monitor, daemon=True, name=f"Risk-{trade_id}")
                    thread.start()
                    self.active_risk_managers.append({'trade_id': trade_id, 'manager': rm, 'thread': thread})
                    logger.info(f"✅ Recovery thread started for {trade_id}")
                except Exception as e:
                    logger.error(f"Failed to recover {trade_id}: {e}")
                    telegram.send(f"❌ Recovery Error: {trade_id}\n{str(e)}", "CRITICAL")
            logger.info(f"✅ Successfully recovered {len(self.active_risk_managers)} trades")
        except Exception as e:
            logger.error(f"Recovery process failed: {e}")
            traceback.print_exc()
            telegram.send(f"❌ Recovery Error: {e}", "CRITICAL")
        finally:
            conn.close()

    def execute_best_mandate(self, analysis: Dict) -> Optional[str]:
        weekly_mandate = analysis['weekly_mandate']
        monthly_mandate = analysis['monthly_mandate']
        mandate = weekly_mandate if weekly_mandate.score.composite > monthly_mandate.score.composite else monthly_mandate
        chain = analysis['weekly_chain'] if mandate == weekly_mandate else analysis['monthly_chain']
        vol_metrics = analysis['vol_metrics']
        struct_metrics = analysis['struct_metrics_weekly'] if mandate == weekly_mandate else analysis['struct_metrics_monthly']
        logger.info(f"Selected mandate: {mandate.expiry_type} {mandate.regime_name} (Score: {mandate.score.composite:.2f})")
        if mandate.expiry_date == date.today():
            logger.warning("Expiry day — no new trades allowed")
            telegram.send("Expiry day — trades blocked", "INFO")
            return None
        if mandate.max_lots == 0:
            logger.info("Mandate is CASH - no trade executed")
            return None
        if circuit_breaker.is_active():
            logger.warning("Circuit breaker active - trade blocked")
            telegram.send("Trade blocked by circuit breaker", "WARNING")
            return None
        if not circuit_breaker.check_daily_trade_limit():
            logger.warning("Daily trade limit reached - trade blocked")
            telegram.send("Daily trade limit reached", "WARNING")
            return None
        deployable = ProductionConfig.BASE_CAPITAL * (mandate.allocation_pct / 100.0)
        if deployable > ProductionConfig.MAX_CAPITAL_PER_TRADE:
            logger.warning(f"Capping capital: ₹{deployable:,.0f} → ₹{ProductionConfig.MAX_CAPITAL_PER_TRADE:,.0f}")
            deployable = ProductionConfig.MAX_CAPITAL_PER_TRADE
            mandate.max_lots = int(deployable / mandate.risk_per_lot) if mandate.risk_per_lot > 0 else 0
        legs, calculated_max_risk = self.strategy_factory.generate(mandate, chain, analysis['lot_size'], vol_metrics, vol_metrics.spot, struct_metrics)
        if not legs:
            logger.error("Failed to generate valid strategy legs")
            telegram.send("Strategy generation failed", "ERROR")
            return None
        logger.info(f"Generated {len(legs)} legs. Exact Max Risk: ₹{calculated_max_risk:,.2f}")
        is_shadow = os.getenv("VG_SHADOW_MODE", "FALSE").upper() == "TRUE"
        filled_legs = []
        if is_shadow:
            logger.info("🎭 SHADOW MODE ACTIVE: Routing to Shadow Engine")
            try:
                filled_legs = self.shadow_engine.execute_strategy(legs)
            except Exception as e:
                logger.error(f"Shadow Execution Failed: {e}")
                traceback.print_exc()
                return None
        else:
            logger.info("💰 LIVE MONEY MODE: Routing to Execution Engine")
            filled_legs = self.execution_engine.execute_strategy(legs)
        if not filled_legs:
            logger.error("Strategy execution failed")
            telegram.send("Execution failed - no position opened", "ERROR")
            return None
        trade_prefix = "SHADOW" if is_shadow else ("PAPER" if ProductionConfig.DRY_RUN_MODE else "LIVE")
        trade_id = f"VG32_{trade_prefix}_{int(datetime.now().timestamp())}"
        self.current_trade_id = trade_id
        entry_premium = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'SELL')
        entry_debit = sum(l['entry_price'] * l['filled_qty'] for l in filled_legs if l['side'] == 'BUY')
        net_premium = entry_premium - entry_debit
        db_writer.save_trade(trade_id, mandate.strategy_type, mandate.expiry_date, filled_legs, net_premium, calculated_max_risk)
        record_trade_open(mandate.strategy_type, mandate.expiry_type, trade_id)
        gtt_ids = []
        if not is_shadow and not ProductionConfig.DRY_RUN_MODE:
            short_legs = [l for l in filled_legs if l['side'] == 'SELL']
            if short_legs:
                logger.info(f"Setting up GTT orders for {len(short_legs)} short legs...")
                for leg in short_legs:
                    sl_price = leg['entry_price'] * 2.0
                    target_price = leg['entry_price'] * 0.30
                    gtt_id = self.execution_engine.place_gtt_order(leg['key'], leg['filled_qty'], 'BUY', round(sl_price, 1), round(target_price, 1))
                    if gtt_id: gtt_ids.append(gtt_id)
                    else: logger.warning(f"GTT placement failed for {leg['key']}")
                if gtt_ids:
                    time.sleep(2)
                    if not self.execution_engine.verify_gtt(gtt_ids):
                        logger.critical("❌ GTT VERIFICATION FAILED - EMERGENCY FLATTEN")
                        telegram.send("🚨 GTT placement failed - FLATTENING POSITION IMMEDIATELY", "CRITICAL")
                        self.execution_engine._flatten_legs(filled_legs)
                        db_writer.log_risk_event("GTT_FAILURE_FLATTEN", "CRITICAL", f"GTT verification failed for trade {trade_id}", "Emergency position closure executed")
                        return None
        else:
            logger.info(f"Skipping GTT setup for {trade_prefix} trade")
        if not is_shadow:
            self.current_risk_manager = RiskManager(self.api_client, filled_legs, mandate.expiry_date, trade_id, gtt_ids)
            risk_thread = threading.Thread(target=self.current_risk_manager.monitor, daemon=True, name="Risk-Manager")
            risk_thread.start()
        logger.info(f"✅ Trade {trade_id} opened successfully")
        return trade_id

    def run_auto_mode(self):
        logger.info("🚀 VOLGUARD AWS SCHEDULER STARTED")
        telegram.send("☁️ Bot active on AWS.", "SYSTEM")
        if not token_manager.ensure_valid_token():
            logger.critical("❌ Failed to obtain valid token on startup")
            telegram.send("🚨 *STARTUP FAILED*\n\nCould not obtain Upstox access token.\nPlease authenticate manually:\n`python volguard.py --authenticate`", "CRITICAL")
            sys.exit(1)
        if not self.session_manager.validate_session(force=True):
            logger.error("Initial Session Check Failed")
            sys.exit(1)
        TIME_BRIEF = dtime(8, 45)
        TIME_AUTH_CHECK = dtime(8, 30)
        TIME_OPEN = dtime(9, 15)
        TIME_CLOSE = dtime(15, 30)
        # NEW: Recover any orphaned trades on startup
        self._recover_active_trades()
        while True:
            try:
                now_ist = datetime.now(ProductionConfig.IST)
                curr_time = now_ist.time()
                today_date = now_ist.date()
                heartbeat.beat()
                if now_ist.second == 0:
                    config_manager.reload_if_changed()
                if today_date.weekday() >= 5:
                    if now_ist.minute == 0 and now_ist.second == 0: logger.info("💤 Weekend.")
                    time.sleep(60); continue
                if not self.check_market_open_status(today_date):
                    if now_ist.minute == 0 and now_ist.second == 0: logger.info("💤 Holiday.")
                    time.sleep(60); continue
                if curr_time >= TIME_AUTH_CHECK and curr_time < TIME_BRIEF:
                    last_check_date = db_writer.get_state('last_token_check_date')
                    if last_check_date != str(today_date):
                        logger.info("🔐 Daily token validity check...")
                        if not token_manager.check_token_validity():
                            logger.warning("⚠️  Token will expire - requesting renewal")
                            if token_manager.request_token_via_telegram():
                                logger.info("✅ Token renewed successfully")
                                db_writer.set_state('last_token_check_date', str(today_date))
                            else:
                                logger.critical("❌ Token renewal failed - HALTING")
                                telegram.send("🚨 *DAILY TOKEN RENEWAL FAILED*\n\nCould not renew Upstox token.\nTrading will not start today.", "CRITICAL")
                        else:
                            logger.info("✅ Token is valid for today")
                            db_writer.set_state('last_token_check_date', str(today_date))
                if curr_time >= TIME_BRIEF and self.last_brief_date != today_date:
                    self.run_morning_brief()
                if TIME_OPEN <= curr_time <= TIME_CLOSE:
                    current_30_block = (now_ist.hour, now_ist.minute // 30)
                    if now_ist.minute % 30 == 0 and self.last_analysis_time != current_30_block:
                        logger.info(f"⏰ Scheduled Analysis: {curr_time.strftime('%H:%M')}")
                        analysis = self.run_analysis()
                        if analysis: self.execute_best_mandate(analysis)
                        self.last_analysis_time = current_30_block
                    if now_ist.minute % 30 == 0 and now_ist.second == 0:
                        if not self.session_manager.validate_session():
                            telegram.send("⚠️ Session Expired.", "WARNING")
                            if not self.session_manager.validate_session(force=True): break
                    if now_ist.minute % 5 == 0 and now_ist.second == 0:
                        report = PositionMonitor(self.api_client, db_writer).reconcile()
                        if report and not report['is_clean']:
                            logger.warning("Position reconciliation found discrepancies")
                    time.sleep(1)
                else:
                    time.sleep(10)
            except KeyboardInterrupt: break
            except Exception as e:
                logger.error(f"Loop Error: {e}")
                time.sleep(60)

# ----------  MAIN  ----------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="VOLGUARD 3.2 – AI-Augmented Production")
    parser.add_argument('--mode', choices=['analysis', 'auto'], default='analysis', help='Run mode')
    parser.add_argument('--skip-confirm', action='store_true', help='Skip confirmation for auto mode')
    parser.add_argument('--export-journal', type=str, help='Export trade journal to directory')
    parser.add_argument('--metrics-port', type=int, default=8000, help='Port for Prometheus metrics')
    parser.add_argument('--authenticate', action='store_true', help='Manually trigger Upstox authentication flow')
    args = parser.parse_args()

    print("="*80)
    print("VOLGUARD 3.2 – AI-AUGMENTED PRODUCTION")
    print("Advanced Option Selling System")
    if ProductionConfig.DRY_RUN_MODE:
        print("🎯 DRY RUN MODE - NO REAL TRADES")
    print("="*80)

    if args.export_journal:
        os.makedirs(args.export_journal, exist_ok=True)
        if db_writer.export_trade_journal(args.export_journal):
            print(f"✅ Trade journal exported to {args.export_journal}")
        else:
            print("❌ Export failed")
        return

    try:
        ProductionConfig.validate()
        logger.info("✅ Configuration validated")
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        sys.exit(1)

    try:
        start_metrics_server(args.metrics_port)
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")

    db_writer.set_state("system_version", "3.2-PRODUCTION")
    db_writer.set_state("startup_time", datetime.now().isoformat())
    db_writer.set_state("dry_run_mode", str(ProductionConfig.DRY_RUN_MODE))
    logger.info("✅ Database initialized (WAL Mode)")

    telegram.send(f"🚀 System Startup\nVersion: 3.2 PRODUCTION\nMode: {args.mode.upper()}\nEnvironment: {ProductionConfig.ENVIRONMENT}\n{'📄 DRY RUN - Paper Trading' if ProductionConfig.DRY_RUN_MODE else '💰 LIVE TRADING'}", "SUCCESS")

    if args.authenticate:
        print("\n" + "="*80)
        print("🔐 MANUAL AUTHENTICATION MODE")
        print("="*80)
        print("\nInitiating Upstox OAuth flow...")
        print("You will receive a Telegram message with an authorization link.\n")
        try:
            ProductionConfig.validate()
            if token_manager.request_token_via_telegram():
                print("\n✅ Authentication successful!")
                print(f"Token valid until: {(datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d 03:30 AM')}")
                print("\nYou can now start the bot:")
                print("  python volguard.py --mode auto")
            else:
                print("\n❌ Authentication failed.")
                print("Please check your Telegram and try again.")
                sys.exit(1)
        except Exception as e:
            print(f"\n❌ Error: {e}")
            traceback.print_exc()
            sys.exit(1)
        return

    orchestrator = TradingOrchestrator()
    try:
        if args.mode == 'analysis':
            logger.info("Running ANALYSIS mode")
            result = orchestrator.run_analysis()
            if result:
                print("\n" + "="*80); print("MARKET ANALYSIS RESULTS"); print("="*80)
                w, m = result['weekly_mandate'], result['monthly_mandate']
                print(f"\n📊 WEEKLY MANDATE"); print(f"Regime: {w.regime_name}"); print(f"Strategy: {w.suggested_structure}"); print(f"Score: {w.score.composite:.2f} ({w.score.confidence})")
                print(f"Allocation: {w.allocation_pct:.1f}% | Max Lots: {w.max_lots}"); print(f"Rationale: {', '.join(w.rationale)}")
                if w.warnings: print(f"Warnings: {', '.join(w.warnings)}")
                print(f"\n📊 MONTHLY MANDATE"); print(f"Regime: {m.regime_name}"); print(f"Strategy: {m.suggested_structure}"); print(f"Score: {m.score.composite:.2f} ({m.score.confidence})")
                print(f"Allocation: {m.allocation_pct:.1f}% | Max Lots: {m.max_lots}"); print(f"Rationale: {', '.join(m.rationale)}")
                if m.warnings: print(f"Warnings: {', '.join(m.warnings)}")
                print("\n" + "="*80)
            else:
                print("❌ Analysis failed")
        elif args.mode == 'auto':
            if ProductionConfig.DRY_RUN_MODE:
                logger.info("🎯 Starting AUTO MODE in DRY RUN (Paper Trading)")
                orchestrator.run_auto_mode()
            else:
                bypass_confirm = os.getenv("VG_AUTO_CONFIRM", "FALSE").upper() == "TRUE"
                if bypass_confirm or args.skip_confirm:
                    logger.warning("⚠️  AUTO CONFIRMATION - LIVE TRADING ENABLED")
                    orchestrator.run_auto_mode()
                else:
                    print("\n" + "="*80); print("⚠️  LIVE AUTO MODE REQUESTED"); print("="*80); print("\nThis will enable live trading with REAL MONEY."); print("Ensure you understand the risks involved."); print("\nType 'I ACCEPT THE RISK' to continue: ")
                    try:
                        user_input = input().strip()
                        if user_input == "I ACCEPT THE RISK":
                            orchestrator.run_auto_mode()
                        else:
                            logger.info("Auto mode cancelled by user"); print("Cancelled.")
                    except EOFError:
                        logger.critical("No input available. Set VG_AUTO_CONFIRM=TRUE or use --skip-confirm"); sys.exit(1)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        traceback.print_exc()
        telegram.send(f"💥 System crashed: {str(e)}", "CRITICAL")
        sys.exit(1)
    finally:
        logger.info("System shutdown sequence initiated")
        heartbeat.stop()
        db_writer.shutdown()
        telegram.send("System shutdown complete", "SYSTEM")
        logger.info("Goodbye.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 User Interrupted (Ctrl+C)")
    except Exception as e:
        print(f"💥 Fatal Crash: {e}")
        traceback.print_exc()
    finally:
        print("🧹 Cleaning up processes...")
        try: db_writer.shutdown()
        except: pass
        try: telegram.shutdown()
        except: pass
        try:
            current_process = psutil.Process()
            children = current_process.children(recursive=True)
            for child in children:
                try:
                    print(f"💀 Killing zombie process: {child.pid}")
                    child.terminate()
                except: pass
        except: pass
        print("👋 Shutdown Complete.")
