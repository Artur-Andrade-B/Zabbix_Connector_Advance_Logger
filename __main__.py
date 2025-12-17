import os
import sys
import time
import json
import socket
import threading
import traceback
import re
import atexit
from queue import Queue, Empty, Full
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo  # type: ignore

import paramiko
from dotenv import load_dotenv
from colorama import init as colorama_init, Fore, Style

# API pieces
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import uvicorn

#################################
# Config / constants
#################################
load_dotenv()

ENV = {
    "SFTP_HOST": os.getenv("SFTP_HOST"),
    "SFTP_PORT": int(os.getenv("SFTP_PORT")),
    "SFTP_USER": os.getenv("SFTP_USER"),
    "SFTP_PASS": os.getenv("SFTP_PASS", ""),
    "SFTP_DIR": os.getenv("SFTP_DIR"),
    "SFTP_FILE_ACTIVE": os.getenv("SFTP_FILE_ACTIVE"),

    "ZBX_SERVER": os.getenv("ZBX_SERVER", ""),
    "ZBX_PORT": int(os.getenv("ZBX_PORT", "10051")),
    "ZBX_HOST": os.getenv("ZBX_HOST", ""),
    "ZBX_BATCH_MAX": int(os.getenv("ZBX_BATCH_MAX", "100")),
    "ZBX_BATCH_INTERVAL_MS": int(os.getenv("ZBX_BATCH_INTERVAL_MS", "1000")),
    "ZBX_FAIL_SPool": os.getenv("ZBX_FAIL_SPool", "/var/spool/asterisk-zbx/queue.jsonl"),

    "POLL_INTERVAL_MS": int(os.getenv("POLL_INTERVAL_MS", "2000")),
    "RATE_LIMIT_EVENTS_PER_SEC": int(os.getenv("RATE_LIMIT_EVENTS_PER_SEC", "50")),
    "MAX_RECONNECT_BACKOFF_MS": int(os.getenv("MAX_RECONNECT_BACKOFF_MS", "60000")),
    "TIMEZONE": os.getenv("TIMEZONE", "America/Sao_Paulo"),
    "IGNORE_AMI_AUTH": os.getenv("IGNORE_AMI_AUTH", "false").lower() == "true",
    "IGNORE_AMI_AUTH_IPS": {ip.strip() for ip in os.getenv("IGNORE_AMI_AUTH_IPS", "").split(",") if ip.strip()},
    "IDLE_EMPTY_POLLS": int(os.getenv("IDLE_EMPTY_POLLS", "3")),

    # API config
    "API_KEY_MON": os.getenv("API_KEY_MON", ""),  # bearer token for monitor API
    "API_PORT": int(os.getenv("API_PORT", "8080")),
    "API_HOST": os.getenv("API_HOST", "0.0.0.0"),
}

BACKLOG_MINUTES = 3
COOLDOWN_SEC = 60
WATCHDOG_STALL_SEC = 5.0

try:
    TZ = ZoneInfo(ENV["TIMEZONE"]) if ENV["TIMEZONE"] else timezone.utc
except Exception:
    _FALLBACK_OFFSETS = {"America/Sao_Paulo": -3}
    off = _FALLBACK_OFFSETS.get(ENV["TIMEZONE"])
    TZ = timezone(timedelta(hours=off)) if off is not None else timezone.utc

SPOOL_PATH = Path(ENV["ZBX_FAIL_SPool"])
STATE_PATH = SPOOL_PATH.with_name("state.json")
SPOOL_PATH.parent.mkdir(parents=True, exist_ok=True)

#################################
# Color / logger infra
#################################
colorama_init()

LOG_LEVEL_COLORS = {
    "NOTICE": Fore.GREEN,
    "WARNING": Fore.YELLOW,
    "ERROR":   Fore.RED,
}
class DailyTerminalLog:
    """
    Writes every terminal line (both logger lines and JSON event lines) to a daily .log file.
    Filename: logs_dd_mm_yy.log
    """
    def __init__(self, directory: Path, prefix: str = "logs"):
        self.dir = directory
        self.prefix = prefix
        self.lock = threading.Lock()
        self.fp = None
        self.day = None
        self._last_err = None

    def path_for(self, dt: Optional[datetime] = None) -> Path:
        dt = dt or datetime.now(TZ)
        return self.dir / f"{self.prefix}_{dt:%d_%m_%y}.log"

    def _ensure_open_locked(self) -> None:
        dt = datetime.now(TZ)
        d = dt.date()
        if self.fp is not None and self.day == d:
            return

        try:
            if self.fp:
                self.fp.close()
        except Exception:
            pass

        p = self.path_for(dt)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            self.fp = open(p, "a", encoding="utf-8", buffering=1)
            self.day = d
            self._last_err = None
        except Exception as e:
            msg = f"[TERMLOG] cannot open {p}: {e}\n"
            if msg != self._last_err:
                self._last_err = msg
                try:
                    sys.__stderr__.write(msg)
                    sys.__stderr__.flush()
                except Exception:
                    pass
            self.fp = None
            self.day = d

    def touch(self) -> None:
        with self.lock:
            self._ensure_open_locked()
            if self.fp:
                try:
                    self.fp.flush()
                except Exception:
                    pass

    def write(self, text: str) -> None:
        if not text:
            return
        with self.lock:
            self._ensure_open_locked()
            if not self.fp:
                return
            try:
                self.fp.write(text)
            except Exception:
                pass

    def close(self) -> None:
        with self.lock:
            try:
                if self.fp:
                    self.fp.close()
            except Exception:
                pass
            self.fp = None
            self.day = None

TERMLOG = DailyTerminalLog(SPOOL_PATH.parent, prefix=os.getenv("TERMLOG_PREFIX", "logs"))
atexit.register(TERMLOG.close)

def terminal_log_path_now() -> Path:
    return TERMLOG.path_for(datetime.now(TZ))

def mirror_terminal_line(text: str) -> None:
    TERMLOG.write(text)

def term_write(text: str, flush: bool = True, mirror: bool = True) -> None:
    """
    Write to stdout and mirror to daily log (exact same text).
    """
    try:
        sys.stdout.write(text)
        if flush:
            sys.stdout.flush()
    except Exception:
        pass
    if mirror:
        mirror_terminal_line(text)

class ThreadLogger(threading.Thread):
    def __init__(self):
        super().__init__(name="logger", daemon=False)
        self.q = Queue()
        self._stop_flag = False

    def stop(self):
        self._stop_flag = True

    def log(self, level: str, src: str, msg: str):
        self.q.put((level, src, msg))

    def run(self):
        while not self._stop_flag:
            try:
                level, src, msg = self.q.get(timeout=0.5)
            except Empty:
                watchdog_scan()
                continue
            color = LOG_LEVEL_COLORS.get(level, "")
            line = f"{color}[{level}] {src}: {msg}{Style.RESET_ALL}\n"
            try:
                term_write(line, flush=True, mirror=True)
            except Exception:
                pass

LOGGER = ThreadLogger()

def log_notice(src, msg): LOGGER.log("NOTICE", src, msg)
def log_warn(src, msg):   LOGGER.log("WARNING", src, msg)
def log_err(src, msg):    LOGGER.log("ERROR", src, msg)

#################################
# Heartbeat tracking
#################################
HEARTBEAT_LOCK = threading.Lock()
HEARTBEAT = {
    # "collector": {"last_tick": float(monotonic), "status": "...", "cycle_id": int}
}

def hb_update(status: str, cycle_id: Optional[int] = None):
    with HEARTBEAT_LOCK:
        h = HEARTBEAT.setdefault("collector", {})
        h["last_tick"] = time.monotonic()
        h["status"] = status
        if cycle_id is not None:
            h["cycle_id"] = cycle_id

def hb_snapshot():
    with HEARTBEAT_LOCK:
        return dict(HEARTBEAT.get("collector", {}))

def watchdog_scan():
    snap = hb_snapshot()
    if not snap:
        return
    last = snap.get("last_tick")
    status = snap.get("status")
    cid = snap.get("cycle_id")
    if last is None:
        return
    age = time.monotonic() - last
    if age > WATCHDOG_STALL_SEC and status == "running":
        log_warn("watchdog", f"collector[{cid}] heartbeat stale {age:.1f}s")
        with HEARTBEAT_LOCK:
            HEARTBEAT["collector"]["status"] = "stalled"
        log_err("watchdog", f"collector[{cid}] stalled")

#################################
# Basic helpers
#################################
def now_tz():
    return datetime.now(TZ)

def now_epoch() -> int:
    return int(time.time())

def safe_print_jsonl(obj: dict):
    try:
        sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    except Exception:
        pass

def safe_append_spool(obj: dict):
    try:
        with open(SPOOL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception as e:
        log_err("io", f"append_spool failed: {e}")

#################################
# Ring buffers for API exposure
#################################
MAX_BUFFER_EVENTS = 200

COLLECTOR_BUF_LOCK = threading.Lock()
COLLECTOR_EVENTS: List[dict] = []  # recent Asterisk events

SERVICE_BUF_LOCK = threading.Lock()
SERVICE_EVENTS: List[dict] = []    # recent external service monitor events

def _buf_append(buf_lock: threading.Lock, buf: List[dict], ev: dict):
    with buf_lock:
        buf.append(ev)
        if len(buf) > MAX_BUFFER_EVENTS:
            # drop oldest
            del buf[0:len(buf) - MAX_BUFFER_EVENTS]

def record_collector_event(ev: dict):
    _buf_append(COLLECTOR_BUF_LOCK, COLLECTOR_EVENTS, ev)

def record_service_event(ev: dict):
    _buf_append(SERVICE_BUF_LOCK, SERVICE_EVENTS, ev)

def snapshot_collector_events(limit: int = 50) -> List[dict]:
    with COLLECTOR_BUF_LOCK:
        return COLLECTOR_EVENTS[-limit:].copy()

def snapshot_service_events(limit: int = 50) -> List[dict]:
    with SERVICE_BUF_LOCK:
        return SERVICE_EVENTS[-limit:].copy()

#################################
# Rate limiter
#################################
class RateLimiter:
    def __init__(self, rate_per_sec: int):
        self.capacity = max(1, rate_per_sec)
        self.tokens = self.capacity
        self.refill_rate = rate_per_sec
        self.last = time.time()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            now = time.time()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

#################################
# Zabbix emitter
#################################
class ZabbixEmitter:
    def __init__(self, server: str, port: int, host: str):
        self.server = server.strip()
        self.port = port
        self.host = host.strip()
        self.batch = []
        self.batch_max = ENV["ZBX_BATCH_MAX"]
        self.batch_interval = ENV["ZBX_BATCH_INTERVAL_MS"] / 1000.0
        self.last_flush = time.time()
        self.lock = threading.Lock()

    def emit(self, event: dict):
        # 1: always mirror to stdout + disk spool
        safe_print_jsonl(event)
        safe_append_spool(event)

        # 2: also mirror into collector buffer if this looks like asterisk.* or asterisk.log.*
        k = event.get("key", "")
        if k.startswith("asterisk."):
            record_collector_event(event)

        # 3: send to Zabbix server if configured
        if not self.server or not self.host:
            return
        with self.lock:
            self.batch.append(event)
            if len(self.batch) >= self.batch_max or (time.time() - self.last_flush) >= self.batch_interval:
                self._flush_locked()

    def emit_service_health(self, event: dict):
        # service side buffer
        record_service_event(event)
        # still push to spool + maybe Zabbix
        safe_print_jsonl(event)
        safe_append_spool(event)
        if not self.server or not self.host:
            return
        with self.lock:
            self.batch.append(event)
            if len(self.batch) >= self.batch_max or (time.time() - self.last_flush) >= self.batch_interval:
                self._flush_locked()

    def flush(self):
        if not self.server or not self.host:
            return
        with self.lock:
            if self.batch:
                self._flush_locked()

    def _flush_locked(self):
        payload = self._format_zbx_payload(self.batch)
        try:
            self._send_to_zbx(payload)
            self._drop_from_spool(len(self.batch))
        except Exception as e:
            log_err("zabbix", f"send failed: {e}")
        finally:
            self.batch.clear()
            self.last_flush = time.time()

    def _format_zbx_payload(self, events: list[dict]) -> bytes:
        data = []
        for ev in events:
            data.append({
                "host": self.host,
                "key": ev.get("key"),
                "value": ev.get("value"),
                "clock": int(datetime.fromisoformat(ev["ts"]).timestamp()),
            })
        obj = {"request": "sender data", "data": data}
        return json.dumps(obj).encode("utf-8")

    def _send_to_zbx(self, body: bytes):
        header = b"ZBXD\x01"
        length_bytes = len(body).to_bytes(8, "little", signed=False)
        packet = header + length_bytes + body
        with socket.create_connection((self.server, self.port), timeout=5) as s:
            s.sendall(packet)
            _ = s.recv(1024)

    def _drop_from_spool(self, n: int):
        try:
            rest = []
            with open(SPOOL_PATH, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i >= n:
                        rest.append(line)
            with open(SPOOL_PATH, "w", encoding="utf-8") as f:
                f.writelines(rest)
        except FileNotFoundError:
            pass
        except Exception as e:
            log_err("io", f"drop_from_spool failed: {e}")

EMITTER = ZabbixEmitter(ENV["ZBX_SERVER"], ENV["ZBX_PORT"], ENV["ZBX_HOST"])
LIMITER = RateLimiter(ENV["RATE_LIMIT_EVENTS_PER_SEC"])

#################################
# Persistent state
#################################
class State:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.data = {
            "files": {},          # "full": {offset:int, mtime:float, tail_buf:str}
            "last_line_ts": None  # ISO watermark of last emitted event
        }
        self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception:
            pass

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        try:
            with self.lock:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self.data, f)
            tmp.replace(self.path)
        except Exception as e:
            log_err("io", f"state save failed: {e}")

STATE = State(STATE_PATH)

def get_watermark_ts() -> Optional[datetime]:
    with STATE.lock:
        w = STATE.data.get("last_line_ts")
    if not w:
        return None
    try:
        return datetime.fromisoformat(w)
    except Exception:
        return None

def maybe_update_watermark(ts: datetime):
    with STATE.lock:
        cur = STATE.data.get("last_line_ts")
        if (cur is None) or (ts.isoformat() > cur):
            STATE.data["last_line_ts"] = ts.isoformat()

#################################
# SFTP Tailer
#################################
class SFTPTailer:
    def __init__(self):
        self.client = None
        self.sftp = None
        self.backoff_ms = 1000

    def connect(self):
        self.close()
        cli = paramiko.SSHClient()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cli.connect(
            hostname=ENV["SFTP_HOST"],
            port=ENV["SFTP_PORT"],
            username=ENV["SFTP_USER"],
            password=ENV["SFTP_PASS"],
            look_for_keys=False,
            allow_agent=False,
            banner_timeout=10,
            auth_timeout=10,
            timeout=10,
        )
        tr = cli.get_transport()
        if tr:
            tr.set_keepalive(15)
        self.client = cli
        self.sftp = cli.open_sftp()
        self.sftp.chdir(ENV["SFTP_DIR"])
        self.backoff_ms = 1000

    def ensure(self):
        try:
            if not self.client or not self.sftp:
                raise RuntimeError("disconnected")
            _ = self.sftp.getcwd()
        except Exception:
            self._reconnect_with_backoff()

    def _reconnect_with_backoff(self):
        start = time.time()
        while True:
            # If cooldown is active, stop reconnect loop immediately.
            if cooldown_start.is_set():
                raise RuntimeError("cooldown_in_progress")

            try:
                self.connect()
                return
            except Exception as e:
                log_err("collector", f"SFTP reconnect failed: {e}")

                # If cooldown started while we were failing, stop immediately.
                if cooldown_start.is_set():
                    raise RuntimeError("cooldown_in_progress")

                time.sleep(self.backoff_ms / 1000.0)
                self.backoff_ms = min(self.backoff_ms * 2, ENV["MAX_RECONNECT_BACKOFF_MS"])

                if time.time() - start > 5:
                    ev = build_simple_event("asterisk.collector.source_down", 1)
                    EMITTER.emit(ev)
                    start = float('inf')

    def stat_full(self):
        self.ensure()
        st = self.sftp.stat(ENV["SFTP_FILE_ACTIVE"])
        return {"mtime": st.st_mtime, "size": st.st_size}

    def _get_file_state(self):
        with STATE.lock:
            fs = STATE.data["files"].setdefault(
                ENV["SFTP_FILE_ACTIVE"],
                {"offset": 0, "mtime": 0.0, "tail_buf": ""}
            )
            fs["offset"] = int(fs.get("offset", 0) or 0)
            fs["mtime"] = float(fs.get("mtime", 0.0) or 0.0)
            fs["tail_buf"] = fs.get("tail_buf", "") or ""
        return fs

    def _set_file_state(self, fs):
        with STATE.lock:
            STATE.data["files"][ENV["SFTP_FILE_ACTIVE"]] = fs

    def _read_region(self, start: int, length: int) -> str:
        with self.sftp.open(ENV["SFTP_FILE_ACTIVE"], "r") as f:  # type: ignore
            f.set_pipelined(True)
            f.seek(start)
            data = f.read(length)
        return data.decode("utf-8", errors="replace")

    def tail_forward_new_lines(self):
        meta = self.stat_full()
        fs = self._get_file_state()

        if meta["size"] < fs["offset"] or meta["mtime"] < fs["mtime"]:
            log_notice("collector", "rotation detected: resetting offset")
            fs["offset"] = 0
            fs["tail_buf"] = ""

        if fs["offset"] >= meta["size"]:
            fs["mtime"] = meta["mtime"]
            self._set_file_state(fs)
            return []

        start_off = fs["offset"]
        want = meta["size"] - fs["offset"]
        chunk = self._read_region(fs["offset"], want)

        fs["offset"] = meta["size"]
        fs["mtime"] = meta["mtime"]

        text = fs["tail_buf"] + chunk
        fs["tail_buf"] = ""
        self._set_file_state(fs)

        if not text:
            return []

        lines = text.splitlines()
        if not text.endswith("\n"):
            if lines:
                with STATE.lock:
                    STATE.data["files"][ENV["SFTP_FILE_ACTIVE"]]["tail_buf"] = lines.pop()

        out = []
        cur_off = start_off
        for line in lines:
            out.append((cur_off, line))
            cur_off += len(line) + 1
        return out

    def read_recent_window(self, minutes_back: int):
        meta = self.stat_full()
        size = meta["size"]

        wm = get_watermark_ts()
        cutoff_ts = now_tz() - timedelta(minutes=minutes_back)
        if wm and wm > cutoff_ts:
            cutoff_ts = wm

        block_size = 65536
        max_bytes = 1024 * 1024
        collected = ""
        pos = max(0, size - block_size)
        fetched_total = 0
        cur_size = size
        while True:
            want = cur_size - pos
            if want <= 0:
                break
            chunk = self._read_region(pos, want)
            collected = chunk + collected
            fetched_total += want
            if pos == 0:
                break
            if fetched_total >= max_bytes:
                break
            cur_size = pos
            pos = max(0, pos - block_size)

        lines = collected.splitlines()
        out_lines = []
        for ln in lines:
            parsed = parse_prefix_full(ln)
            if not parsed:
                continue
            ts = parsed["ts"]
            if ts > cutoff_ts:
                out_lines.append(ln)

        results = []
        fake_off = 0
        for l in out_lines:
            results.append((fake_off, l))
            fake_off += len(l) + 1
        return results

    def close(self):
        try:
            if self.sftp:
                self.sftp.close()
        except Exception:
            pass
        try:
            if self.client:
                self.client.close()
        except Exception:
            pass
        finally:
            self.sftp = None
            self.client = None

#################################
# Parsing and event extraction
#################################
RE_PREFIX = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] "
    r"(?P<level>[A-Z]+)\[(?P<pid>\d+)(?:\]\[(?P<callid>[^\]]+)\])?\] "
    r"(?P<src>[^:]+): (?P<msg>.*)$"
)

RE_AMI_FAIL        = re.compile(r"Connect attempt from '(?P<ip>[^']+)' unable to authenticate")
RE_HTTP_LOGIN      = re.compile(r"HTTP Manager '(?P<user>[^']+)' logged on from (?P<ip>\S+)")
RE_HTTP_LOGOFF     = re.compile(r"HTTP Manager '(?P<user>[^']+)' logged off from (?P<ip>\S+)")
RE_PEER_UNREACH    = re.compile(r"Peer '(?P<peer>[^']+)' is now UNREACHABLE")
RE_PEER_REACH      = re.compile(r"Peer '(?P<peer>[^']+)' is now Reachable\.")
RE_QUEUE_ENTER     = re.compile(r'Queue\("[^"]+",\s*"(?P<queue>[^,\"]+)')
RE_CALLED_AGENT    = re.compile(r"Called SIP/(?P<agent>\d+)")
RE_ANSWERED        = re.compile(r"SIP/(?P<agent>\d+)-[\w]+ answered ")
RE_SUSSURRO        = re.compile(r"Playing 'sussurro/(?P<var>[^']+)\.slin'")
RE_BRIDGE_JOIN     = re.compile(r"joined 'simple_bridge' basic-bridge <(?P<br>[^>]+)>")
RE_MOH_START       = re.compile(r"Started music on hold")
RE_MOH_STOP        = re.compile(r"Stopped music on hold")
RE_USER_EVENT_AT   = re.compile(r'UserEvent\("[^"]*",\s*"AMBEV_ATENDIMENTO,Fila:(?P<q>[^,]+),Ramal:(?P<r>[^"]*)"\)')
RE_AUTODESTRUCT_BYE= re.compile(r'Autodestruct .* \(Method: BYE\)')
RE_LOGGER_RESTART  = re.compile(r'Asterisk Queue Logger restarted')
RE_SQL_ERR_QUEUELOG= re.compile(r"res_config_odbc\.c: SQL Prepare failed! \[INSERT INTO queue_log")

class Correlator:
    def __init__(self):
        self.last_queue_by_pid = {}
        self.called_at = {}
        self.queue_enter_ts = {}
        self.bridge_members = set()

    def mark_queue(self, pid: str, queue: str, ts: datetime):
        self.last_queue_by_pid[pid] = queue
        self.queue_enter_ts[queue] = ts

    def mark_called(self, agent: str, ts: datetime):
        self.called_at[agent] = ts

    def mark_answer(self, agent: str, ts: datetime):
        pass

    def mark_bridge_join(self, agent: str):
        self.bridge_members.add(agent)

CORR = Correlator()

def parse_prefix_full(line: str):
    m = RE_PREFIX.match(line)
    if not m:
        return None
    d = m.groupdict()
    raw_ts = d["ts"]
    try:
        ts = datetime.strptime(raw_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except Exception:
        ts = now_tz()
    return {
        "raw_ts": raw_ts,
        "ts": ts,
        "pid": d.get("pid"),
        "level": d.get("level") or "",
        "srcmod": d.get("src") or "",
        "msg": d.get("msg") or "",
    }

def parse_prefix(line: str):
    parsed = parse_prefix_full(line)
    if not parsed:
        return None
    return {
        "raw_ts": parsed["raw_ts"],
        "ts": parsed["ts"],
        "pid": parsed["pid"],
        "msg": parsed["msg"],
        "level": parsed["level"],
        "srcmod": parsed["srcmod"],
    }

def build_simple_event(key: str, value, tags: dict | None = None, src: dict | None = None, raw: str | None = None):
    base = {
        "ts": now_tz().isoformat(),
        "key": key,
        "value": value,
        "tags": tags or {},
        "kpi": None,
        "src": src or {},
    }
    if raw is not None:
        base["raw"] = raw
    return base

def build_event(ts: datetime, key: str, value, tags: dict | None = None, src: dict | None = None, raw: str | None = None):
    ev = {
        "ts": ts.isoformat(),
        "key": key,
        "value": value,
        "tags": tags or {},
        "kpi": None if not isinstance(value, (int, float)) or value in (0, 1) else True,
        "src": src or {},
    }
    if raw is not None:
        ev["raw"] = raw
    return ev

def nearest_agent(ts: datetime) -> Optional[str]:
    best = None
    best_dt = None
    for agent, dt in CORR.called_at.items():
        if abs((ts - dt).total_seconds()) <= 5:
            if best_dt is None or dt > best_dt:
                best = agent
                best_dt = dt
    return best

def parse_agent_from_src(msg: str) -> Optional[str]:
    m = re.search(r"Channel SIP/(?P<agent>\d+)-", msg)
    return m.group("agent") if m else None

def extract_events(offset: int, line: str):
    p = parse_prefix(line)
    if not p:
        return []

    ts = p["ts"]
    wm = get_watermark_ts()
    if wm and ts <= wm:
        return []

    pid = p["pid"]
    msg = p["msg"]
    level = p["level"]
    srcmod = p["srcmod"]

    src_info = {
        "file": ENV["SFTP_FILE_ACTIVE"],
        "offset": offset,
        "raw_ts": p["raw_ts"],
    }

    out = []

    # generic WARNING / ERROR surface
    if level in ("WARNING", "ERROR"):
        clip_msg = msg if len(msg) <= 200 else (msg[:197] + "...")
        sev_key = f'asterisk.log.severity[{level},{srcmod}]'
        sev_tags = {
            "severity": level,
            "module": srcmod,
            "text": clip_msg,
        }
        out.append(build_event(ts, sev_key, 1, sev_tags, src_info, line))

    m = RE_AMI_FAIL.search(msg)
    if m:
        ip = m.group("ip")
        if not (ENV["IGNORE_AMI_AUTH"] or (ENV["IGNORE_AMI_AUTH_IPS"] and ip in ENV["IGNORE_AMI_AUTH_IPS"])):
            out.append(build_event(ts, f"asterisk.ami.auth_fail[{ip}]", 1,
                                   {"srcip": ip}, src_info, line))

    m = RE_HTTP_LOGIN.search(msg)
    if m:
        out.append(build_event(ts, f"asterisk.ami.http_login[{m.group('ip')},{m.group('user')}]", 1,
                               {"srcip": m.group("ip"), "user": m.group("user")},
                               src_info, line))

    m = RE_HTTP_LOGOFF.search(msg)
    if m:
        out.append(build_event(ts, f"asterisk.ami.http_logout[{m.group('ip')},{m.group('user')}]", 1,
                               {"srcip": m.group("ip"), "user": m.group("user")},
                               src_info, line))

    m = RE_PEER_UNREACH.search(msg)
    if m:
        peer = m.group("peer")
        out.append(build_event(ts, f"asterisk.peer.unreachable[{peer}]", 1,
                               {"peer": peer}, src_info, line))

    m = RE_PEER_REACH.search(msg)
    if m:
        peer = m.group("peer")
        out.append(build_event(ts, f"asterisk.peer.reachable[{peer}]", 1,
                               {"peer": peer}, src_info, line))

    m = RE_QUEUE_ENTER.search(msg)
    if m:
        q = m.group("queue")
        CORR.mark_queue(pid, q, ts)
        out.append(build_event(ts, f"asterisk.call.enterqueue[{q}]", 1,
                               {"queue": q}, src_info, line))

    m = RE_CALLED_AGENT.search(msg)
    if m:
        agent = m.group("agent")
        CORR.mark_called(agent, ts)
        busy = agent in CORR.bridge_members
        key = f"asterisk.flag.agent_busy_offer[{agent}]" if busy \
              else f"asterisk.queue.called_agent[{agent}]"
        out.append(build_event(ts, key, 1, {"agent": agent}, src_info, line))

    m = RE_ANSWERED.search(msg)
    if m:
        agent = m.group("agent")
        CORR.mark_answer(agent, ts)
        q = CORR.last_queue_by_pid.get(pid)

        out.append(build_event(
            ts,
            f"asterisk.call.answered[{q or ''},{agent}]",
            1,
            {"queue": q or "", "agent": agent},
            src_info,
            line
        ))

        if q and q in CORR.queue_enter_ts:
            latency_ms = int((ts - CORR.queue_enter_ts[q]).total_seconds() * 1000)
            out.append(build_event(
                ts,
                f"asterisk.kpi.answer_latency_ms[{q}]",
                latency_ms,
                {"queue": q}
            ))
            if latency_ms > 15000:
                out.append(build_event(
                    ts,
                    f"asterisk.flag.answer_latency_high[{q}]",
                    1,
                    {"queue": q, "latency_ms": latency_ms}
                ))

        if agent in CORR.called_at:
            ring_ms = int((ts - CORR.called_at[agent]).total_seconds() * 1000)
            out.append(build_event(
                ts,
                f"asterisk.kpi.ring_ms[{q or ''},{agent}]",
                ring_ms,
                {"queue": q or "", "agent": agent}
            ))

    m = RE_SUSSURRO.search(msg)
    if m:
        var = m.group("var")
        ag = nearest_agent(ts)
        q = CORR.last_queue_by_pid.get(pid)
        out.append(build_event(
            ts,
            f"asterisk.sussurro.played[{q or ''},{ag or ''},{var}]",
            1,
            {"queue": q or "", "agent": ag or "", "variant": var},
            src_info,
            line
        ))

    m = RE_BRIDGE_JOIN.search(msg)
    if m:
        br = m.group("br")
        ag = parse_agent_from_src(msg)
        if ag:
            CORR.mark_bridge_join(ag)
        out.append(build_event(
            ts,
            f"asterisk.call.bridge_join[{br}]",
            1,
            {"bridge": br, "agent": ag or ""},
            src_info,
            line
        ))

    if RE_MOH_START.search(msg):
        out.append(build_event(ts, "asterisk.moh.start", 1, {}, src_info, line))

    if RE_MOH_STOP.search(msg):
        out.append(build_event(ts, "asterisk.moh.stop", 1, {}, src_info, line))

    if RE_AUTODESTRUCT_BYE.search(msg):
        out.append(build_event(ts, "asterisk.autodestruct.bye", 1, {}, src_info, line))

    if RE_LOGGER_RESTART.search(msg):
        out.append(build_event(ts, "asterisk.logger.restart", 1, {}, src_info, line))

    if RE_SQL_ERR_QUEUELOG.search(msg):
        out.append(build_event(ts, "asterisk.sql_error.odbc_queue_log", 1, {}, src_info, line))

    m = RE_USER_EVENT_AT.search(msg)
    if m:
        qv = m.group("q")
        rv = m.group("r")
        out.append(build_event(
            ts,
            f"asterisk.userevent.atendimento[{qv},{rv}]",
            1,
            {"queue": qv, "agent": rv},
            src_info,
            line
        ))

    if out:
        maybe_update_watermark(ts)

    return out

def emit_events(ev_list: list[dict]):
    for e in ev_list:
        if LIMITER.allow():
            EMITTER.emit(e)
        else:
            sup = build_simple_event(
                f"asterisk.counter.suppressed[{e.get('key')}]",
                1,
                {"key": e.get("key")}
            )
            EMITTER.emit(sup)

#################################
# Coordination events
#################################
cooldown_start = threading.Event()

#################################
# Parser worker (one cycle per trigger)
#################################
class ParserWorker(threading.Thread):
    def __init__(self, cycle_id: int):
        super().__init__(name=f"collector-{cycle_id}", daemon=True)
        self.cycle_id = cycle_id
        self._stop_local = False

    def run(self):
        hb_update("running", self.cycle_id)
        log_notice("collector", f"cycle {self.cycle_id} start")
        tailer = SFTPTailer()
        try:
            tailer._reconnect_with_backoff()
        except Exception as e:
            if str(e) == "cooldown_in_progress":
                log_warn("collector", f"cycle {self.cycle_id} initial connect aborted (cooldown in progress)")
                hb_update("idle", self.cycle_id)
                return

            log_err("collector", f"cycle {self.cycle_id} initial connect failed: {e}\n{traceback.format_exc()}")
            hb_update("error", self.cycle_id)
            cooldown_start.set()
            return

        up_ev = build_simple_event("asterisk.collector.source_up", 1)
        EMITTER.emit(up_ev)
        log_notice("collector", f"cycle {self.cycle_id} source_up emitted")

        try:
            warm_lines = tailer.read_recent_window(BACKLOG_MINUTES)
            log_notice("collector", f"cycle {self.cycle_id} warm replay candidate lines: {len(warm_lines)}")
            cnt = 0
            for off, line in warm_lines:
                evs = extract_events(off, line)
                if evs:
                    emit_events(evs)
                    cnt += 1
            if cnt > 0:
                log_notice("collector", f"cycle {self.cycle_id} warm replay events emitted: {cnt}")
            STATE.save()
        except Exception as e:
            log_err("collector", f"cycle {self.cycle_id} warm replay error: {e}\n{traceback.format_exc()}")

        empty_polls = 0
        poll_interval = ENV["POLL_INTERVAL_MS"] / 1000.0

        while not self._stop_local:
            hb_update("running", self.cycle_id)
            try:
                tailer.ensure()
                got_any_new_bytes = False
                got_any_emitted = False

                new_lines = tailer.tail_forward_new_lines()
                for off, line in new_lines:
                    got_any_new_bytes = True
                    evs = extract_events(off, line)
                    if evs:
                        emit_events(evs)
                        got_any_emitted = True

                STATE.save()

                if got_any_emitted:
                    empty_polls = 0
                    log_notice("collector", f"cycle {self.cycle_id} poll emitted new events")
                else:
                    empty_polls += 1
                    if got_any_new_bytes:
                        log_warn("collector", f"cycle {self.cycle_id} poll new bytes but filtered")
                    else:
                        log_warn("collector", f"cycle {self.cycle_id} poll no new bytes")

                if empty_polls >= ENV["IDLE_EMPTY_POLLS"]:
                    log_notice("collector", f"cycle {self.cycle_id} idle threshold hit. requesting cooldown")
                    hb_update("idle", self.cycle_id)
                    break

            except Exception as e:
                log_err("collector", f"cycle {self.cycle_id} poll error: {e}\n{traceback.format_exc()}")
                try:
                    tailer._reconnect_with_backoff()
                    log_warn("collector", f"cycle {self.cycle_id} reconnected after error")
                except Exception as e2:
                    log_err("collector", f"cycle {self.cycle_id} reconnect failed: {e2}\n{traceback.format_exc()}")
                    hb_update("error", self.cycle_id)
                    cooldown_start.set()
                    break

            time.sleep(poll_interval)

        try:
            tailer.close()
        except Exception:
            pass

        cooldown_start.set()

#################################
# Cooldown worker
#################################
class CooldownWorker(threading.Thread):
    def __init__(self):
        super().__init__(name="cooldown", daemon=True)

    def run(self):
        while True:
            cooldown_start.wait()

            log_notice("cooldown", f"starting cooldown {COOLDOWN_SEC}s")
            for remaining in range(COOLDOWN_SEC, 0, -1):
                hb_update("idle")
                bar_msg = f"{Fore.YELLOW}Cooldown {remaining:2d}s{Style.RESET_ALL}"
                try:
                    sys.stdout.write("\r" + bar_msg)
                    sys.stdout.flush()
                except Exception:
                    pass
                time.sleep(1)

            try:
                sys.stdout.write("\r" + Fore.GREEN + "Cooldown complete        " + Style.RESET_ALL + "\n")
                sys.stdout.flush()
            except Exception:
                pass

            log_notice("cooldown", "cooldown finished")
            cooldown_start.clear()
            scheduler_request_new_cycle()

#################################
# Scheduler / controller
#################################
SCHED_LOCK = threading.Lock()
SCHED_STATE = {
    "cycle_id": 0,
    "active_thread": None,
    "counter":0,
}
def clear_screen():
    import subprocess
    command = 'cls' if os.name == 'nt' else 'clear'
    subprocess.run([command], shell=True)

def scheduler_request_new_cycle():
    with SCHED_LOCK:
        if SCHED_STATE["counter"] >= 10:
            clear_screen()
            SCHED_STATE["counter"] = 0
        SCHED_STATE["counter"] += 1
        SCHED_STATE["cycle_id"] += 1
        cid = SCHED_STATE["cycle_id"]
        pw = ParserWorker(cycle_id=cid)
        SCHED_STATE["active_thread"] = pw
        log_notice("scheduler", f"launching collector cycle {cid}")
        hb_update("running", cid)
        pw.start()

class SchedulerThread(threading.Thread):
    def __init__(self):
        super().__init__(name="scheduler", daemon=True)

    def run(self):
        scheduler_request_new_cycle()

        while True:
            time.sleep(0.2)
            with SCHED_LOCK:
                pw = SCHED_STATE["active_thread"]
                cid = SCHED_STATE["cycle_id"]
            if pw is None:
                continue

            if not pw.is_alive():
                hb_update("idle", cid)
                continue

            snap = hb_snapshot()
            if snap.get("status") == "stalled" and snap.get("cycle_id") == cid:
                log_err("scheduler", f"collector[{cid}] stalled. abandoning and forcing cooldown")
                cooldown_start.set()
                hb_update("error", cid)


#################################
# API auth helper
#################################
BEARER_RE = re.compile(r"^\s*Bearer\s+(.+)\s*$", re.I)

def _auth_or_401(request: Request):
    m = BEARER_RE.match(request.headers.get("authorization", ""))
    if not m:
        raise HTTPException(status_code=401, detail="unauthorized")
    token = m.group(1)
    if token != ENV["API_KEY_MON"] or not token:
        raise HTTPException(status_code=401, detail="unauthorized")

#################################
# API endpoints
#################################
app = FastAPI()

@app.get("/health")
async def health(request: Request):
    _auth_or_401(request)
    snap = hb_snapshot()
    with SCHED_LOCK:
        cid = SCHED_STATE["cycle_id"]
        alive = (SCHED_STATE["active_thread"].is_alive()
                 if SCHED_STATE["active_thread"] is not None else False)
    return JSONResponse({
        "ok": True,
        "time": now_epoch(),
        "cycle_id": cid,
        "collector_alive": alive,
        "collector_status": snap.get("status"),
        "collector_last_hb_age_sec": (
            round(time.monotonic() - snap["last_tick"], 2)
            if snap.get("last_tick") else None
        )
    })

@app.get("/events/asterisk")
async def get_asterisk_events(request: Request, limit: int = 50):
    _auth_or_401(request)
    data = snapshot_collector_events(limit=limit)
    return JSONResponse({
        "time": now_epoch(),
        "count": len(data),
        "events": data,
    })

@app.get("/events/service")
async def get_service_events(request: Request, limit: int = 50):
    _auth_or_401(request)
    data = snapshot_service_events(limit=limit)
    return JSONResponse({
        "time": now_epoch(),
        "count": len(data),
        "events": data,
    })

@app.post("/events/service")
async def post_service_event(request: Request):
    _auth_or_401(request)
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")

    svc = body.get("service", "")
    status = body.get("status", "")
    detail = body.get("detail", "")
    kpi_flag = body.get("kpi", None)

    if not svc or not isinstance(svc, str):
        raise HTTPException(status_code=400, detail="missing_service")
    if not status or not isinstance(status, str):
        raise HTTPException(status_code=400, detail="missing_status")

    # Build normalized event
    # Example key: service.monitor[report_sgg]
    ev = {
        "ts": now_tz().isoformat(),
        "key": f"service.monitor[{svc}]",
        "value": status,
        "tags": {
            "service": svc,
            "status": status,
            "detail": detail[:200] if isinstance(detail, str) else "",
        },
        "kpi": bool(kpi_flag),
        "src": {"type": "service_push"},
    }

    # Record in memory buffer
    record_service_event(ev)

    # Push to zabbix spool / socket
    EMITTER.emit_service_health(ev)

    return JSONResponse({"ok": True, "accepted": ev})

#################################
# main startup
#################################
if __name__ == "__main__":
    # start logger
    LOGGER.start()
    log_notice("controller", "logger thread started")

    # start cooldown worker
    cdw = CooldownWorker()
    cdw.start()
    log_notice("controller", "cooldown thread started")

    # start scheduler
    sched = SchedulerThread()
    sched.start()
    log_notice("controller", "scheduler thread started")

    # run API in main thread
    try:
        uvicorn.run(
            app,
            host=ENV["API_HOST"],
            port=ENV["API_PORT"],
            log_level="warning",
        )
    except KeyboardInterrupt:
        log_warn("controller", "KeyboardInterrupt received. exiting main loop")
        LOGGER.stop()
        time.sleep(0.5)
