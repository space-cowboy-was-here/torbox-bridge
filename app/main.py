"""
TorBox Bridge - Main Application Entry Point

This Flask application acts as a proxy bridge between TorBox (cloud download service)
and local media automation tools like Sonarr/Radarr. It emulates both SABnzbd (Usenet)
and qBittorrent (Torrent) APIs to provide a unified interface for cloud downloading.
"""

import os
import sys
import time
import requests
import shutil
import threading
import uuid
import sqlite3
import json
import subprocess
import logging
import re
from tempfile import mkdtemp
from datetime import datetime
from functools import wraps
from flask import Flask, jsonify, request, Response, render_template, session, redirect, url_for, g
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

# --- CONFIGURATION ---
DEBUG_MODE = os.getenv("FLASK_DEBUG", "False").lower() == "true"
CONFIG_DIR = os.getenv("CONFIG_DIR", os.path.join(os.getcwd(), "config"))
DB_PATH = os.path.join(CONFIG_DIR, "torbox_sab.db")

# Helper to load DB-backed config with Env var fallback
def get_setting(conn, key, default):
    try:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        if row and row['value']: return row['value']
    except: pass
    return default

# Initial Load (will be refreshed in loops)
API_KEY = None
SAB_API_KEY = "torbox123"
MAX_CONCURRENT_DL = 3
STUCK_TIMEOUT = 3600
DL_DIR = os.getenv("DL_DIR", os.path.join(os.getcwd(), "downloads"))

# Internal Config
TORBOX_URL = "https://api.torbox.app/v1/api"
LOG_PATH = os.path.join(CONFIG_DIR, "bridge.log")

# Global State
ACTIVE_DOWNLOADS = {} # Map nzo_id -> subprocess.Popen
SESSION = requests.Session() # Reuse connections
SESSION.mount('https://', requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10))

# Setup Logging (File + Console)
if not os.path.exists(CONFIG_DIR): os.makedirs(CONFIG_DIR)

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s',
                   handlers=[
                       logging.FileHandler(LOG_PATH),
                       logging.StreamHandler()
                   ])

# Reduce noise from Flask/Werkzeug
logging.getLogger('werkzeug').setLevel(logging.ERROR)
logger = logging.getLogger()


if DEBUG_MODE:
    logger.setLevel(logging.DEBUG)
    logger.info("DEBUG MODE ENABLED - Verbose Logging Active")
    # Silence chatty libraries even in debug mode
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


# --- DATABASE STATE ---
def load_config():
    global API_KEY, SAB_API_KEY, MAX_CONCURRENT_DL, STUCK_TIMEOUT
    try:
        if not os.path.exists(DB_PATH): return
        conn = get_db_connection()
        try:
            API_KEY = get_setting(conn, "torbox_api_key", API_KEY)
            SAB_API_KEY = get_setting(conn, "sab_api_key", SAB_API_KEY)
            val_concurrent = get_setting(conn, "max_concurrent", str(MAX_CONCURRENT_DL))
            if val_concurrent: MAX_CONCURRENT_DL = int(val_concurrent)
            val_stuck = get_setting(conn, "stuck_timeout", str(STUCK_TIMEOUT))
            if val_stuck: STUCK_TIMEOUT = int(val_stuck)
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"Error loading config: {e}")

def init_db():
    try:
        if not os.path.exists(CONFIG_DIR): os.makedirs(CONFIG_DIR)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS queue 
                     (nzo_id TEXT PRIMARY KEY, torbox_id TEXT, filename TEXT, category TEXT, status TEXT, 
                      size REAL, progress REAL, added_at REAL, path TEXT, failure_reason TEXT, last_updated REAL,
                      speed REAL DEFAULT 0, eta REAL DEFAULT 0, is_paused INTEGER DEFAULT 0, priority INTEGER DEFAULT 0, avg_speed REAL DEFAULT 0)''')
        try:
            c.execute("ALTER TABLE queue ADD COLUMN last_updated REAL")
        except: pass

        try:
            c.execute("ALTER TABLE queue ADD COLUMN output_path TEXT")
        except: pass

        try:
            c.execute("ALTER TABLE queue ADD COLUMN is_paused INTEGER DEFAULT 0")
        except: pass
        
        try:
            c.execute("ALTER TABLE queue ADD COLUMN speed REAL DEFAULT 0")
        except: pass

        try:
            c.execute("ALTER TABLE queue ADD COLUMN eta REAL DEFAULT 0")
        except: pass

        try:
            c.execute("ALTER TABLE queue ADD COLUMN avg_speed REAL DEFAULT 0")
        except: pass
        
        c.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE, password_hash TEXT)''')
        
        # Enable WAL mode for better concurrency
        c.execute("PRAGMA journal_mode=WAL;")
        
        # Reset any stuck local downloads from previous runs
        c.execute("UPDATE queue SET status='Cloud_Done' WHERE status='Downloading_Local'")
        
        # Add priority column if missing (Migration)
        try:
            c.execute("ALTER TABLE queue ADD COLUMN priority INTEGER DEFAULT 0")
        except: pass

        # Add avg_speed column if missing (Migration)
        try:
            c.execute("ALTER TABLE queue ADD COLUMN avg_speed REAL")
        except: pass
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Init Error: {e}")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;") 
    return conn

# --- HELPERS FROM ORIGINAL SCRIPT ---
def check_for_archives(files):
    """Check if file list contains archives (rar, zip, etc)"""
    archive_pattern = re.compile(r'\.(rar|zip|7z|tar|gz|r\d{2}|part\d+\.rar)$', re.IGNORECASE)
    for f in files:
        if archive_pattern.search(f['short_name']): return True
    return False

def has_video_file(files):
    """Check if a playable video file exists (>50MB)"""
    for f in files:
        if f['size'] > 50 * 1024 * 1024: # 50MB
            if f['short_name'].lower().endswith(('.mkv', '.mp4', '.avi', '.ts', '.m4v')):
                return True
    return False

def format_time(seconds):
    if not seconds or seconds <= 0: return "0:00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}"

# --- LOGGING UTILITY ---
class DownloadLogger:
    def __init__(self, nzo_id):
        self.nzo_id = nzo_id
        self.buffer = []
        self.log_path = os.path.join(CONFIG_DIR, "logs", f"{nzo_id}.log")
        
    def log(self, level, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Include nzo_id in the stored log entry too, for clarity if files are merged
        entry = f"{timestamp} - [{self.nzo_id}] - {level} - {message}"
        self.buffer.append(entry)
        
        # Also print to main console if DEBUG_MODE or warn/error
        # We prefix with nzo_id for context in main log
        if DEBUG_MODE or level in ["INFO", "WARNING", "ERROR", "CRITICAL"]:
            logger_func = getattr(logger, level.lower(), logger.info)
            logger_func(f"[{self.nzo_id}] {message}")

    def info(self, msg): self.log("INFO", msg)
    def debug(self, msg): self.log("DEBUG", msg)
    def warning(self, msg): self.log("WARNING", msg)
    def error(self, msg): self.log("ERROR", msg)
    
    def save(self):
        try:
            log_dir = os.path.dirname(self.log_path)
            if not os.path.exists(log_dir): os.makedirs(log_dir)
            with open(self.log_path, 'w') as f:
                f.write("\n".join(self.buffer))
        except Exception as e:
            logger.error(f"Failed to save download log: {e}")

    def discard(self):
        self.buffer = []


# --- TORBOX API WRAPPERS ---
def torbox_add_torrent(magnet=None, file_obj=None, filename=None):
    if DEBUG_MODE: logger.debug(f"Adding Torrent: {filename} (Magnet: {magnet is not None})")
    
    url = f"{TORBOX_URL}/torrents/createtorrent"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    data = {}
    files = {}

    if magnet:
        data = {'magnet': magnet}
    elif file_obj:
        if not filename: filename = 'upload.torrent'
        files = {'file': (filename, file_obj, 'application/x-bittorrent')}
    
    try:
        if files:
            # Files upload needs no Content-Type header (requests sets boundary)
            r = SESSION.post(url, headers=headers, files=files, timeout=30)
        else:
            r = SESSION.post(url, headers=headers, data=data, timeout=30)
            
        if DEBUG_MODE: logger.debug(f"TorBox Add Torrent Response: {r.status_code} - {r.text[:200]}")

        if r.status_code == 200:
            resp = r.json()
            if resp.get('success'):
                # TorBox returns { ..., data: { torrent_id: ..., hash: ..., name: ... } }
                return resp['data'], None
            else:
                return None, resp.get('detail') or resp.get('error')
        return None, f"HTTP {r.status_code}: {r.text}"
    except Exception as e:
        return None, str(e)

def torbox_add_nzb(file_obj, filename):
    url = f"{TORBOX_URL}/usenet/createusenetdownload"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    if DEBUG_MODE: logger.debug(f"Adding NZB to TorBox: {filename}")

    if not API_KEY: return None, "Missing API Key"
    if not filename.lower().endswith('.nzb'): filename += '.nzb'

    files = {'file': (filename, file_obj, 'application/x-nzb')}
    try:
        r = SESSION.post(url, headers=headers, files=files, timeout=30)
        if DEBUG_MODE: logger.debug(f"TorBox Add Response: {r.status_code} - {r.text[:500]}") # Log first 500 chars

        # Handle rate limiting / non-JSON error bodies cleanly
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            msg = "rate limit exceeded"
            if retry_after:
                msg += f" (Retry-After: {retry_after}s)"
            return None, msg

        try:
            data = r.json()
        except:
            # Some TorBox errors are plain text
            return None, f"Invalid JSON: {r.text[:200]}"


        if data.get('success'):
            tid = data['data'].get('usenetdownload_id') or data['data'].get('usenet_id') or data['data'].get('id')
            return tid, None
        
        error_detail = data.get('detail') or data.get('error', f"Unknown Error {r.status_code}")
        return None, error_detail
    except Exception as e:
        return None, str(e)

def torbox_get_list():
    # Fetch BOTH Usenet and Torrents to be safe
    items = []
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    # Usenet
    try:
        r = SESSION.get(f"{TORBOX_URL}/usenet/mylist?bypass_cache=true", headers=headers, timeout=10)
        if r.status_code == 200: 
            data = r.json().get('data', [])
            items.extend(data)
            # if DEBUG_MODE and data: logger.debug(f"Fethed {len(data)} Usenet items")
    except Exception as e:
        if DEBUG_MODE: logger.debug(f"Usenet List Error: {e}")
    
    # Torrents (just in case)
    try:
        r = SESSION.get(f"{TORBOX_URL}/torrents/mylist?bypass_cache=true", headers=headers, timeout=10)
        if r.status_code == 200: 
            data = r.json().get('data', [])
            items.extend(data)
            # if DEBUG_MODE and data: logger.debug(f"Fethed {len(data)} Torrent items")
    except Exception as e:
        if DEBUG_MODE: logger.debug(f"Torrent List Error: {e}")
    
    return items

def torbox_control(t_id, operation):
    if DEBUG_MODE: logger.debug(f"TorBox Control: {operation} on {t_id}")
    # Try both endpoints since we might not know the type for sure

    headers = {"Authorization": f"Bearer {API_KEY}"}
    for ep in ['usenet/controlusenetdownload', 'torrents/controltorrent']:
        try:
            payload = {"usenet_id": t_id, "operation": operation} if 'usenet' in ep else {"torrent_id": t_id, "operation": operation}
            SESSION.post(f"{TORBOX_URL}/{ep}", headers=headers, json=payload, timeout=5)
        except: pass

def request_download_link(t_id, file_id):
    # Try Usenet first, then Torrent
    for type_slug in ['usenet', 'torrents']:
        id_key = "usenet_id" if type_slug == "usenet" else "torrent_id"
        url = f"{TORBOX_URL}/{type_slug}/requestdl?token={API_KEY}&{id_key}={t_id}&file_id={file_id}"
        try:
            r = SESSION.get(url, timeout=10)
            if r.status_code == 200 and r.json().get('success'):
                link = r.json().get('data')
                if DEBUG_MODE: logger.debug(f"Got Download Link ({type_slug}): {link}")
                return link
            elif DEBUG_MODE:
                logger.debug(f"Request DL Fail ({type_slug}): {r.text}")
        except Exception as e:
            if DEBUG_MODE: logger.debug(f"Request DL Error ({type_slug}): {e}")
    return None


def safe_folder_name(nzo_id: str, filename: str = None, torbox_name: str = None) -> str:
    """Create a stable, filesystem-safe folder name for a job.

    Prefer TorBox's display name (cleaner), fall back to the NZB filename stem,
    and finally the nzo_id. Keeps paths short to avoid OS/path-length issues.
    """
    candidate = torbox_name or filename or nzo_id
    # Strip extension(s)
    if candidate:
        candidate = re.sub(r'\.(nzb|torrent)$', '', candidate, flags=re.I)
    # Werkzeug secure_filename removes/normalizes unsafe chars
    cleaned = secure_filename(candidate) if candidate else ""
    cleaned = cleaned.strip("._-")  # avoid weird dot/empty names
    if not cleaned:
        cleaned = nzo_id
    return cleaned[:120]  # keep it reasonably short

# --- LOCAL DOWNLOAD THREAD ---
def local_download_thread(nzo_id, tid, category, folder_name, file_url, file_name, expected_size):
    conn = get_db_connection()
    c = conn.cursor()
    process = None
    
    # Init Logger
    dl_log = DownloadLogger(nzo_id)
    dl_log.info(f"Initialized Local Download Thread for {file_name}")

    try:
        final_dir = os.path.join(DL_DIR, category, folder_name)
        if not os.path.exists(final_dir): os.makedirs(final_dir)
        
        dl_log.info(f"Starting Local Download: {file_name} -> {final_dir}")
        dl_log.debug(f"Aria2 URL: {file_url}")
        
        try:
            row_limit = c.execute("SELECT value FROM settings WHERE key='speed_limit'").fetchone()
            global_limit_mb = float(row_limit['value']) if row_limit else 0
        except:
            global_limit_mb = 0

        # Define Dynamic Limit Calculator
        def get_dynamic_limit():
            if global_limit_mb <= 0: return 0
            
            # Count active downloads
            # We filter for those that are actually running (in ACTIVE_DOWNLOADS)
            active_count = len(ACTIVE_DOWNLOADS)
            if nzo_id not in ACTIVE_DOWNLOADS: active_count += 1 # Include self if not yet added
            if active_count < 1: active_count = 1
            
            limit_per_bytes = int((global_limit_mb * 1024 * 1024) / active_count)
            return max(1024, limit_per_bytes) # Min 1KB

        # Initial Limit
        current_limit_bytes = get_dynamic_limit()
        
        dl_log.info(f"Speed Limit Strategy: {global_limit_mb} MB/s Global")
        if global_limit_mb > 0:
            dl_log.info(f"Dynamic Limit: {current_limit_bytes/1024/1024:.2f} MB/s (Active: {len(ACTIVE_DOWNLOADS) + 1})")
        
        # Determine Full File Path
        final_dir = os.path.join(DL_DIR, category, folder_name) # Ensure defined here
        if not os.path.exists(final_dir): os.makedirs(final_dir)
        full_path = os.path.join(final_dir, file_name)

        # ETA & Average Speed Tracking (Session Persisted)
        start_time_dl = time.time()
        start_byte_offset = 0
        try:
            if os.path.exists(full_path):
                 start_byte_offset = os.stat(full_path).st_blocks * 512
        except: pass

        smoothed_speed = 0
        displayed_eta = 0 # Track displayed value for smoothing
        
        # Outer loop to handle restarts for speed limit updates
        while True:
            limit_arg = []
            if global_limit_mb > 0:
                # Recalculate before starting
                current_limit_bytes = get_dynamic_limit()
                limit_arg = [f"--max-download-limit={current_limit_bytes}"]

            # Use --file-allocation=none so file grows gradually
            # Tuning for stability: User-Agent to avoid blocks, Lowest Speed Limit to force reconnects on stalls
            cmd = [
                "aria2c", 
                "-x", "16", "-s", "16", # MATCH REFERENCE SCRIPT: 16 connections
                "-k", "1M", # MATCH REFERENCE SCRIPT: 1M split
                "-c", # Continue
                "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", 
                "--connect-timeout=10", # Timeout connection attempts after 10s
                "--max-tries=0", # Infinite retries
                "--retry-wait=2", # Wait 2s between retries
            ] + limit_arg + ["--file-allocation=none", "-d", final_dir, "-o", file_name, file_url]
            
            dl_log.debug(f"Command: {' '.join(cmd)}")
            
            # Capture stderr
            aria_err_file = os.path.join(final_dir, "aria_stderr.log")
            err_file_handle = open(aria_err_file, "a") # Append mode for restarts
            
            process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=err_file_handle, text=True)
            ACTIVE_DOWNLOADS[nzo_id] = process
            
            # Reset Loop Metrics (But not Session Metrics)
            last_size = 0
            last_check_time = time.time()
            try:
                if os.path.exists(full_path): last_size = os.stat(full_path).st_blocks * 512
            except: pass
            
            process_finished_naturally = False
            last_db_check = 0 # Ensure we check immediately first time
            restart_requested = False
            
            while process.poll() is None:
                cur_time = time.time()
                
                # Check Global Speed Limit from DB (Every 2 seconds)
                if cur_time - last_db_check > 2:
                    try:
                        conn_check = get_db_connection()
                        row_limit = conn_check.execute("SELECT value FROM settings WHERE key='speed_limit'").fetchone()
                        new_global_limit = float(row_limit['value']) if row_limit else 0
                        
                        # Update global var if changed
                        if new_global_limit != global_limit_mb:
                             # If moving from Unlimited (0) to Limited (>0), or Limited to Limited:
                             # We update variable so dynamic logic picks it up
                             dl_log.info(f"Detected Speed Limit Change: {global_limit_mb} -> {new_global_limit}")
                             global_limit_mb = new_global_limit
                        conn_check.close()
                    except: pass
                    last_db_check = cur_time

                # Check for dynamic speed limit adjustment
                if global_limit_mb > 0:
                    new_limit = get_dynamic_limit()
                    # If limit changed by more than 15%, restart process to apply
                    if abs(new_limit - current_limit_bytes) > (current_limit_bytes * 0.15):
                        dl_log.info(f"Adjusting speed limit: {current_limit_bytes/1024:.0f}K -> {new_limit/1024:.0f}K")
                        restart_requested = True
                        process.terminate()
                        try: process.wait(timeout=5)
                        except: process.kill()
                        break # Break inner loop, will restart in outer loop
                elif global_limit_mb == 0 and current_limit_bytes > 0:
                     # Switch from Limited to Unlimited -> Restart
                     dl_log.info("Switching to Unlimited Speed")
                     restart_requested = True
                     process.terminate()
                     try: process.wait(timeout=5)
                     except: process.kill()
                     break

                if os.path.exists(full_path):
                    # cur_size = os.path.getsize(full_path) # Logical size (misleading for sparse files)
                    try:
                        # Use allocated blocks to estimate actual downloaded bytes on sparse filesystem
                        cur_size = os.stat(full_path).st_blocks * 512 
                    except:
                        cur_size = 0

                    cur_time = time.time()
                    
                    if expected_size and expected_size > 0:
                        progress = (cur_size / expected_size) * 100
                    if progress > 100: progress = 99.9 # Cap if filesystem overhead exceeds size
                    
                    # Calculate Speed
                    time_diff = cur_time - last_check_time
                    inst_speed = 0
                    
                    if time_diff >= 1.0: # Check once per second for more stable math
                        if cur_size > last_size:
                             inst_speed = (cur_size - last_size) / time_diff
                        
                        # Apply smoothing for "Current Speed" display (responsive but not jittery)
                        if smoothed_speed == 0: smoothed_speed = inst_speed
                        else: smoothed_speed = (inst_speed * 0.2) + (smoothed_speed * 0.8)
                        
                        # Calculate Long-Term Average for ETA (Very stable)
                        # We track from when THIS loop started or persisted
                        session_bytes = cur_size - start_byte_offset
                        session_time = cur_time - start_time_dl
                        
                        avg_speed = 0
                        if session_time > 5 and session_bytes > 0: # Warmup 5s
                            avg_speed = session_bytes / session_time
                        else:
                            avg_speed = smoothed_speed # Fallback during warmup

                        # Calculate Raw ETA based on Long-Term Average
                        remaining = expected_size - cur_size
                        raw_eta = remaining / avg_speed if avg_speed > 0 else 0
                        
                        # Smart ETA Logic (Countdown Feel)
                        # Only update ETA if it drops (good news) or if it increases significantly (bad news)
                        if displayed_eta == 0:
                            displayed_eta = raw_eta
                        else:
                            # What ETA *should* be if it just ticked down naturally
                            natural_eta = max(0, displayed_eta - time_diff)
                            
                            if raw_eta < natural_eta:
                                # Speedup! Allowed to drop faster.
                                displayed_eta = raw_eta
                            else:
                                # Slowdown or noise.
                                diff = raw_eta - natural_eta
                                
                                # Only accept increase if it is SIGNIFICANT.
                                # Threshold: At least 3 minutes (180s) OR 10% of total time, whichever is LARGER (up to a cap)
                                # Actually, user complains about 1-2m bumps on 70m download.
                                # Let's say: Must be > 10% change. 70m * 10% = 7 minutes. That solves it.
                                # But for short downloads (2m), 10% is 12s. That might be too sensitive?
                                # Let's use: max(60, natural_eta * 0.10)
                                # If ETA 70m (4200s): Needs > 420s (7m) jump.
                                # If ETA 2m (120s): Needs > 60s jump.
                                
                                tolerance = max(60, natural_eta * 0.10)
                                
                                if diff > tolerance:
                                     # dl_log.info -> dl_log.debug as requested to reduce noise
                                     dl_log.debug(f"ETA adjusted up significantly (+{int(diff)}s) due to speed drop.")
                                     displayed_eta = raw_eta
                                else:
                                     # Fake the countdown to keep it smooth
                                     displayed_eta = natural_eta

                        # Log on every update for high resolution debugging
                        # dl_log.debug(f"Progress: {progress:.1f}% Cur: {smoothed_speed/1024/1024:.2f}MB/s Avg: {avg_speed/1024/1024:.2f}MB/s ETA: {int(eta)}s")
                        
                        try:
                            # We store smoothed_speed as 'speed' because that's what UI speedometer expects
                            # We store 'eta' derived from smart logic for stability
                            # We also store avg_speed for UI use
                            c.execute("UPDATE queue SET progress=?, speed=?, eta=?, last_updated=?, avg_speed=? WHERE nzo_id=?", 
                                    (progress, smoothed_speed, displayed_eta, cur_time, avg_speed, nzo_id))
                            conn.commit()
                        except sqlite3.OperationalError: pass
                    
                        last_check_time = cur_time
                        last_size = cur_size # Ensure we update last_size only after processing iteration
                time.sleep(0.5) # Loop freq

            # Check if we intentionally terminated to apply a new speed limit
            if restart_requested:
                # Close file handles and continue outer loop (restart aria2)
                try: err_file_handle.close()
                except: pass
                if nzo_id in ACTIVE_DOWNLOADS: del ACTIVE_DOWNLOADS[nzo_id]
                continue

            # Otherwise, process finished on its own (success or real failure)
            if process.poll() is not None:
                process_finished_naturally = True
                break # Break Outer Loop

            # Defensive fallback (shouldn't happen): restart
            try: err_file_handle.close()
            except: pass
            if nzo_id in ACTIVE_DOWNLOADS: del ACTIVE_DOWNLOADS[nzo_id]
            continue

        # Close the stderr log file handle
        try: err_file_handle.close()
        except: pass
            
        if process.returncode == 0:
            dl_log.info(f"Local Download Complete: {file_name}")
            # Status -> Importing (Waiting for Sonarr/Radarr to move the file)
            c.execute("UPDATE queue SET status='Importing', progress=100, speed=0, eta=0, path=? WHERE nzo_id=?", (full_path, nzo_id))
            # torbox_control(tid, 'delete') # Disabled deletion based on user preference
            dl_log.discard() # Success = Delete logs
        else:
            # Check if it was paused (killed intentionally)
            row = c.execute("SELECT is_paused FROM queue WHERE nzo_id=?", (nzo_id,)).fetchone()
            if row and row['is_paused']:
                dl_log.info(f"Local Download Paused: {file_name}")
                c.execute("UPDATE queue SET status='Paused_Local', speed=0, eta=0 WHERE nzo_id=?", (nzo_id,))
                dl_log.save()
            else:
                # Read error from log file
                try:
                    with open(aria_err_file, 'r') as f:
                        err = f.read().strip()
                except: err = ""

                # If err is empty, try to get stdout or generic code
                if not err: 
                    err = f"Process exited with code {process.returncode}"
                    
                dl_log.error(f"Aria2 Failed: {err}")
                c.execute("UPDATE queue SET status='Failed', failure_reason=? WHERE nzo_id=?", ("Check Log", nzo_id))
                dl_log.save()
             
        conn.commit()
    except Exception as e:
        dl_log.error(f"Download Thread Error: {e}")
        dl_log.save()
        try:
            c.execute("UPDATE queue SET status='Failed', failure_reason=? WHERE nzo_id=?", (f"Check Log", nzo_id))
            conn.commit()
        except: pass
    finally:
        if nzo_id in ACTIVE_DOWNLOADS: del ACTIVE_DOWNLOADS[nzo_id]
        conn.close()

# --- HELPER FUNCTIONS ---
def send_discord_notification(message):
    try:
        conn = get_db_connection()
        webhook_url = get_setting(conn, "discord_webhook", "")
        conn.close()
        
        if webhook_url:
            payload = {"content": message}
            # Use a thread to not block the caller
            def _send():
                try: requests.post(webhook_url, json=payload, timeout=10)
                except Exception as e: logging.error(f"Discord Notification Failed: {e}")
            threading.Thread(target=_send).start()
    except: pass

# --- BACKGROUND WORKER ---
def download_worker():
    if DEBUG_MODE: logger.info("Background Worker Started")
    polling_interval = 15 # Default idle

    while True:
        try:
            # OPTIMIZATION: Check Local DB first. If queue is empty, don't hit TorBox API.
            check_conn = get_db_connection()
            try:
                active_count = check_conn.execute("SELECT COUNT(*) FROM queue WHERE status NOT IN ('Completed', 'Failed')").fetchone()[0]
            finally:
                check_conn.close()

            if active_count == 0:
                if DEBUG_MODE and int(time.time()) % 60 == 0:
                    logger.debug("Queue empty. Idle.")
                time.sleep(15)
                continue

            cloud_items = torbox_get_list()
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Get Global Pause State
            row = cursor.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
            global_pause = (row and row['value'] == '1')
            
            cursor.execute("SELECT * FROM queue WHERE status NOT IN ('Completed', 'Failed')")
            local_items = {row['torbox_id']: dict(row) for row in cursor.fetchall()}
            
            for c_item in cloud_items:
                # Normalize ID to string
                tid = str(c_item.get('id') or c_item.get('usenetdownload_id') or c_item.get('torrent_id'))
                
                if tid in local_items:
                    local = local_items[tid]
                    
                    # If local download is active, let the thread handle updates
                    if local['status'] in ['Downloading_Local', 'Importing']:
                        if DEBUG_MODE and int(time.time()) % 10 == 0:
                            logger.debug(f"Skipping {local['filename']} (active local download/import)")
                        continue

                    # If Paused_Local, wait user input (or unpause)
                    if local['status'] == 'Paused_Local':
                         continue

                    # --- SMART STATUS LOGIC (PORTED FROM ORIGINAL SCRIPT) ---
                    raw_state = c_item.get('download_state') or c_item.get('status') or c_item.get('state') or 'unknown'
                    state = raw_state.lower()
                    
                    new_status = 'Downloading' # Default
                    
                    # 1. Check for Processing States (WAIT, don't download yet)
                    if any(x in state for x in ['processing', 'zipping', 'repairing', 'moving', 'unpacking']):
                        if DEBUG_MODE: logger.debug(f"Item {tid} state {state} -> Processing...")
                        new_status = 'Downloading' # Keep as downloading so Sonarr waits
                    
                    # 2. Check for Duplicate (Adopt original if possible)
                    elif 'duplicate' in state:
                        # Find the original file in the cloud list
                        original = next((x for x in cloud_items if x['name'] == c_item['name'] and x.get('download_finished')), None)
                        if original:
                            # Switch tracking to the original ID
                            new_tid = str(original.get('id'))
                            if DEBUG_MODE: logger.info(f"Duplicate Found. Swapping {tid} -> {new_tid}")
                            cursor.execute("UPDATE queue SET torbox_id=? WHERE torbox_id=?", (new_tid, tid))
                            tid = new_tid # Update local var for next loop
                            c_item = original # Swap item data
                            state = 'completed'
                        else:
                            logger.error(f"Item {tid} is duplicate but original copy not found.")
                            new_status = 'Failed'
                            
                    # 3. Check for Completion
                    if state == 'completed' or c_item.get('download_finished'):
                        files = c_item.get('files', [])
                        
                        # WAIT if files list is empty (still processing)
                        if not files:
                            if DEBUG_MODE: logger.debug(f"Item {tid} completed but has no files yet.")
                            new_status = 'Downloading'
                        
                        # WAIT if files are archives and no video (still unpacking)
                        elif check_for_archives(files) and not has_video_file(files):
                            if DEBUG_MODE: logger.debug(f"Item {tid} completed but still unpacking archives.")
                            new_status = 'Downloading' 
                        
                        else:
                            # Actually ready!
                            if local['status'] != 'Downloading_Local' and local['status'] != 'Paused_Local':
                                if local['status'] != 'Cloud_Done':
                                    logger.info(f"Item {tid} ({local['filename']}) is Ready for Transfer!")
                                new_status = 'Cloud_Done'

                    elif any(x in state for x in ['error', 'failed', 'stalled', 'broken', 'aborted', 'killed', 'removed']):
                         # Capture cloud failure details
                         reason = f"{state} (Cloud Download)"
                         # Try to get more info from TorBox item
                         if c_item.get('detail'): reason += f" - {c_item.get('detail')}"
                         
                         logger.error(f"Item {tid} reported failure state: {reason}")
                         
                         # Create a log file for this failure too
                         nzo_id = local['nzo_id']
                         cf_log = DownloadLogger(nzo_id)
                         cf_log.error(f"TorBox Cloud Download Failed")
                         cf_log.info(f"State: {state}")
                         cf_log.info(f"Details: {c_item}")
                         cf_log.save()
                         
                         new_status = 'Failed'
                         cursor.execute("UPDATE queue SET failure_reason=? WHERE nzo_id=?", ("Check Log", nzo_id))

                    # Update DB (with timeout tracking)
                    progress = c_item.get('progress', 0) * 100
                    size_bytes = c_item.get('size', 0)
                    speed = float(c_item.get('speed', 0))
                    eta = float(c_item.get('eta', 0))
                    
                    # Check for progress updates
                    last_updated = local.get('last_updated') or local['added_at']
                    # If progress changed or status changed, update timestamp
                    if abs(progress - local['progress']) > 0.1 or local['status'] != new_status:
                        last_updated = time.time()
                        if DEBUG_MODE and local['status'] != new_status: 
                            logger.debug(f"Updating Status {tid}: {local['status']} -> {new_status}")

                        
                    cursor.execute("UPDATE queue SET progress=?, status=?, size=?, speed=?, eta=?, last_updated=? WHERE torbox_id=?", 
                                   (progress, new_status, size_bytes, speed, eta, last_updated, tid))
            
            # --- CHECK IMPORT STATUS ---
            cursor.execute("SELECT * FROM queue WHERE status='Importing'")
            for r in cursor.fetchall():
                f_path = r['path']
                # If file is missing, it means Sonarr moved/deleted it
                if f_path and not os.path.exists(f_path):
                    logger.info(f"File {f_path} gone. Marking {r['filename']} as Imported.")
                    cursor.execute("UPDATE queue SET status='Completed' WHERE nzo_id=?", (r['nzo_id'],))
                    send_discord_notification(f"✅ **Imported**: {r['filename']}")
                # If no path stored, assume completed
                elif not f_path:
                     cursor.execute("UPDATE queue SET status='Completed' WHERE nzo_id=?", (r['nzo_id'],))

            # Check for Timeouts (Stuck items)
            current_time = time.time()
            cursor.execute('''UPDATE queue SET status='Failed', failure_reason='Timeout/Stuck' 
                              WHERE status NOT IN ('Completed', 'Failed') 
                              AND (? - COALESCE(last_updated, added_at)) > ?''', 
                           (current_time, STUCK_TIMEOUT))
            
            conn.commit()

            # --- DOWNLOAD PROCESS ---
            # 1. Check active downloads
            cursor.execute("SELECT COUNT(*) FROM queue WHERE status='Downloading_Local'")
            active_count = cursor.fetchone()[0]

            if not global_pause and active_count < MAX_CONCURRENT_DL:
                limit = MAX_CONCURRENT_DL - active_count
                # Prioritize paused items? For now order by added_at
                # Only pick items that are NOT paused (Highest Priority First)
                cursor.execute("SELECT * FROM queue WHERE status = 'Cloud_Done' AND is_paused=0 ORDER BY priority DESC, added_at ASC LIMIT ?", (limit,))
                to_download = cursor.fetchall()

                for row in to_download:
                    nzo_id = row['nzo_id']
                    tid = row['torbox_id']
                    category = row['category']
                    
                    cursor.execute("UPDATE queue SET status='Downloading_Local', progress=0 WHERE nzo_id=?", (nzo_id,))
                    conn.commit()
                    
                    # Re-find item in cloud list
                    found_item = next((x for x in cloud_items if str(x.get('id')) == tid or str(x.get('usenetdownload_id')) == tid), None)
                    
                    if found_item:
                        files = found_item.get('files', [])
                        target_file = None
                        
                        # Find largest video file
                        for f in files:
                            if f['short_name'].lower().endswith(('.mkv', '.mp4', '.avi', '.ts')):
                                if not target_file or f['size'] > target_file['size']:
                                    target_file = f
                        
                        if not target_file and files:
                            target_file = max(files, key=lambda x: x['size'])

                        if target_file:
                            dl_link = request_download_link(tid, target_file['id'])
                            if dl_link:
                                # Start Thread
                                t = threading.Thread(target=local_download_thread, 
                                                    args=(nzo_id, tid, category, safe_folder_name(nzo_id, row['filename'], found_item.get('name')), dl_link, target_file['short_name'], target_file['size']))
                                t.start()
                            else:
                                cursor.execute("UPDATE queue SET status='Failed', failure_reason='No DL Link' WHERE nzo_id=?", (nzo_id,))
                        else:
                            cursor.execute("UPDATE queue SET status='Failed', failure_reason='No File Found' WHERE nzo_id=?", (nzo_id,))
                    else:
                        cursor.execute("UPDATE queue SET status='Failed', failure_reason='Item Gone' WHERE nzo_id=?", (nzo_id,))
                    conn.commit()
            
            conn.close()

            # --- DYNAMIC POLLING FREQUENCY ---
            # Re-check updated status to decide next sleep duration
            chk_conn = get_db_connection()
            try:
                # Active Cloud Downloads (High frequency needed)
                active_cloud = chk_conn.execute("SELECT COUNT(*) FROM queue WHERE status IN ('Downloading', 'Queued')").fetchone()[0]
                # Any active items at all (Medium frequency)
                any_active = chk_conn.execute("SELECT COUNT(*) FROM queue WHERE status NOT IN ('Completed', 'Failed')").fetchone()[0]
                
                if active_cloud > 0:
                    polling_interval = 1 # Very fast for active cloud downloads
                elif any_active > 0:
                     polling_interval = 5 # Moderate for local transfers/cleanup
                else:
                    polling_interval = 15 # Idle
            finally:
                chk_conn.close()
                
        except Exception as e:
            logger.error(f"Worker Error: {e}")
            polling_interval = 30 # Back off on error
            
        time.sleep(polling_interval)

# --- FLASK SABNZBD API ---
# Determine template directory
template_dir = '/app/templates'
if not os.path.exists(template_dir):
    # Fallback to local 'templates' folder relative to script (for local testing)
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')

app = Flask(__name__, template_folder=template_dir)
app.secret_key = os.environ.get("SECRET_KEY", "torbox-bridge-secret-key-change-me")

# --- AUTH MIDDLEWARE ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    conn = get_db_connection()
    error = None
    try:
        # Check if any user exists (Setup Mode)
        user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        setup_mode = (user_count == 0)

        if request.method == 'POST':
            username = request.form['username']
            password = request.form['password']
            
            if setup_mode:
                # Create Account
                if not username or not password:
                    error = 'Username and Password required.'
                else:
                    hashed = generate_password_hash(password)
                    conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?)", (username, hashed))
                    conn.commit()
                    
                    # Auto login
                    user = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
                    session['user_id'] = user['id']
                    session['username'] = username
                    return redirect(url_for('ui_index'))
            else:
                # Login
                user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
                if user and check_password_hash(user['password_hash'], password):
                    session['user_id'] = user['id']
                    session['username'] = user['username']
                    return redirect(url_for('ui_index'))
                else:
                    error = 'Invalid credentials'

        return render_template('login.html', error=error, setup_mode=setup_mode)
    finally:
        conn.close()

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# Validate Template Directory on Startup
ui_file = os.path.join(template_dir, 'index.html')
if os.path.exists(ui_file):
    try:
        with open(ui_file, 'r') as f: pass
        logger.info(f"UI Template successfully loaded from {ui_file}")
    except Exception as e:
        logger.error(f"UI Template exists at {ui_file} but is NOT READABLE: {e}")
else:
    logger.error(f"UI Template NOT FOUND at {ui_file}. Checked dir: {template_dir}")
    if os.path.exists(template_dir):
        logger.error(f"Folder contents: {os.listdir(template_dir)}")

@app.route('/')
@login_required
def ui_index():
    if not API_KEY:
        return redirect(url_for('ui_settings_page'))
    return render_template('index.html')

@app.route('/ui/data')
@login_required
def ui_data():
    conn = get_db_connection()
    try:
        # Get Disk Space
        total, used, free = shutil.disk_usage(DL_DIR)
        
        size = free
        disk_unit = 'B'
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if size < 1024.0:
                disk_unit = unit
                break
            size /= 1024.0
            disk_unit = unit # Update unit if we continue
            
        disk_space_str = f"{size:.2f} {disk_unit} Free"

        # Get Global Pause
        row_pause = conn.execute("SELECT value FROM settings WHERE key='pause_all'").fetchone()
        global_paused = (row_pause and row_pause['value'] == '1')
        
        # Get Speed Limit
        row_limit = conn.execute("SELECT value FROM settings WHERE key='speed_limit'").fetchone()
        speed_limit = float(row_limit['value']) if row_limit else 0

        # Get Active Queue
        rows = conn.execute("SELECT * FROM queue WHERE status NOT IN ('Completed', 'Failed') ORDER BY added_at ASC").fetchall()
        queue_items = [dict(row) for row in rows]
        
        # Get History
        hist_rows = conn.execute("SELECT * FROM queue WHERE status IN ('Completed', 'Failed') ORDER BY last_updated DESC LIMIT 20").fetchall()
        history_items = [dict(row) for row in hist_rows]

        # Calculate Global Speed
        global_speed = sum((r['speed'] or 0) for r in queue_items)
        
        return jsonify({
            "queue": queue_items,
            "history": history_items,
            "global_speed": global_speed,
            "global_paused": global_paused,
            "speed_limit": speed_limit,
            "disk_space": disk_space_str,
            "current_max_slots": MAX_CONCURRENT_DL # Send defaults for debug, UI can't see the one used in thread though
        })
    finally:
        conn.close()

@app.route('/ui/logs')
@login_required
def ui_logs():
    if os.path.exists(LOG_PATH):
        try:
            # Default to 500 lines if not specified
            lines = int(request.args.get('lines', 500))
        except: lines = 500
        
        try:
            with open(LOG_PATH, 'r') as f:
                # Use tail-like logic: read lines and keep last N
                content_lines = f.readlines()
                last_lines = "".join(content_lines[-lines:])
                return render_template('log_view.html', title="System Log", content=last_lines, is_global=True)
        except Exception as e:
            return f"Error reading log: {e}", 500
            
    return "Log file empty or not found", 404

@app.route('/ui/logs/clear', methods=['POST'])
@login_required
def ui_clear_logs():
    try:
        open(LOG_PATH, 'w').close()
        return jsonify({"status": True})
    except Exception as e:
        return jsonify({"status": False, "error": str(e)})

@app.route('/ui/factory_reset', methods=['POST'])
@login_required
def ui_factory_reset():
    conn = get_db_connection()
    try:
        logging.info("STARTING FACTORY RESET...")
        # 1. Stop all active
        global ACTIVE_DOWNLOADS
        for nzo_id, proc in list(ACTIVE_DOWNLOADS.items()):
            try: proc.terminate()
            except: pass
        ACTIVE_DOWNLOADS.clear()

        # 2. Drop Tables (Settings + Queue)
        conn.execute("DELETE FROM queue")
        conn.execute("DELETE FROM settings") 
        conn.commit()
        
        # 4. Trigger restart
        def _restart_app():
            time.sleep(1)
            python = sys.executable
            os.execl(python, python, *sys.argv)
        threading.Thread(target=_restart_app).start()
        
        return jsonify({"status": True})
    except Exception as e:
        logging.error(f"Factory Reset failed: {e}")
        return jsonify({"status": False, "error": str(e)})
    finally:
        conn.close()

@app.route('/ui/logs/<nzo_id>')
@login_required
def ui_download_log(nzo_id):
    log_file = os.path.join(CONFIG_DIR, 'logs', f"{nzo_id}.log")
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            content = f.read()
            return render_template('log_view.html', title=f"Log: {nzo_id}", content=content, is_global=False)
    return "Log file not found", 404

@app.route('/ui/restart', methods=['POST'])
@login_required
def ui_restart():
    logging.warning("User requested application restart via UI.")
    def _restart():
        time.sleep(1)
        os._exit(1)
    threading.Thread(target=_restart).start()
    return jsonify({"status": "restarting", "message": "Application is restarting..."})

@app.route('/ui/pause_all', methods=['POST'])
@login_required
def ui_pause_all():
    conn = get_db_connection()
    try:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('pause_all', '1')")
        # Mark all active/queued items as paused
        conn.execute("UPDATE queue SET is_paused=1 WHERE status IN ('Downloading_Local', 'Cloud_Done', 'Queued', 'Downloading')")
        conn.commit()
        
        # Kill active downloads
        global ACTIVE_DOWNLOADS
        for nzo_id, proc in list(ACTIVE_DOWNLOADS.items()):
            try:
                proc.terminate()
            except: pass
            
        return jsonify({"status": True})
    finally:
        conn.close()

@app.route('/ui/resume_all', methods=['POST'])
@login_required
def ui_resume_all():
    conn = get_db_connection()
    try:
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('pause_all', '0')")
        # Unpause everyone
        conn.execute("UPDATE queue SET is_paused=0")
        # Resume paused local downloads to ready state
        conn.execute("UPDATE queue SET status='Cloud_Done' WHERE status='Paused_Local'")
        conn.commit()
        return jsonify({"status": True})
    finally:
        conn.close()

@app.route('/ui/item/<nzo_id>/pause', methods=['POST'])
@login_required
def ui_pause_item(nzo_id):
    conn = get_db_connection()
    try:
        conn.execute("UPDATE queue SET is_paused=1 WHERE nzo_id=?", (nzo_id,))
        conn.commit()
        
        # If currently downloading, kill it
        if nzo_id in ACTIVE_DOWNLOADS:
            try:
                ACTIVE_DOWNLOADS[nzo_id].terminate()
            except: pass
            
        return jsonify({"status": True})
    finally:
        conn.close()


# --- QBITTORRENT API IMPLEMENTATION (Mimics v2) ---

@app.route('/api/v2/auth/login', methods=['POST'])
def api_v2_auth_login():
    username = request.form.get('username')
    password = request.form.get('password')
    
    conn = get_db_connection()
    try:
        # Check against DB
        user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if user and check_password_hash(user['password_hash'], password):
            resp = Response('Ok.', 200)
            resp.set_cookie('SID', 'torbox_bridge_sid', httponly=True)
            return resp
        logger.warning(f"Login failed for user: {username}")
        return 'Fails.', 401
    except Exception as e:
        logger.error(f"Login API Error: {e}")
        return 'Fails.', 401
    finally:
        conn.close()

@app.route('/api/v2/app/webapiVersion', methods=['GET'])
def api_v2_app_webapi_version():
    return '2.8.19' # qBit version string

@app.route('/api/v2/app/preferences', methods=['GET', 'POST'])
def api_v2_app_preferences():
    # Return dummy preferences focused on download paths
    return jsonify({
        'save_path': '/downloads',
        'temp_path': '/downloads/incomplete',
        'scan_dirs': {},
        'proxy_type': 0,
        'listen_port': 6881,
        'dht': False,
        'pex': False,
        'lsd': False,
        'natpmp': False,
        'upnp': False
    })

@app.route('/api/v2/transfer/info', methods=['GET'])
def api_v2_transfer_info():
    conn = get_db_connection()
    try:
        # Calculate global speed
        rows = conn.execute("SELECT speed FROM queue WHERE status='Downloading_Local'").fetchall()
        dl_speed = sum(r['speed'] for r in rows if r['speed'])
        return jsonify({
            'dl_info_speed': int(dl_speed),
            'dl_info_data': 0, # Total downloaded session data (not tracked)
            'up_info_speed': 0,
            'up_info_data': 0,
            'dl_rate_limit': 0,
            'up_rate_limit': 0,
            'dht_nodes': 0,
            'connection_status': 'connected'
        })
    finally:
        conn.close()

@app.route('/api/v2/torrents/categories', methods=['GET'])
def api_v2_torrents_categories():
    return jsonify({
        "movies": {"name": "movies", "savePath": "/downloads/movies"},
        "tv": {"name": "tv", "savePath": "/downloads/tv"}
    })

@app.route('/api/v2/torrents/add', methods=['POST'])
def api_v2_torrents_add():
    # Handle both multipart form uploads (.torrent) and form fields (magnet)
    # qBittorrent sends 'urls' for magnets, 'torrents' for files
    urls = request.form.get('urls') # Magnet links (newline separated)
    category = request.form.get('category', 'default')
    paused = request.form.get('paused', 'false') == 'true'
    
    added_hashes = []
    
    # 1. Handle Magnet Links
    if urls:
        for magnet in urls.split('\n'):
            magnet = magnet.strip()
            if not magnet: continue
            
            # Extract hash immediately for tracking?
            # Creating torrent on TorBox returns hash usually
            data, error = torbox_add_torrent(magnet=magnet)
            if data:
                # Use hash as nzo_id for qBit compatibility
                nzo_id = data.get('hash')
                tid = str(data.get('torrent_id'))
                name = data.get('name', 'Unknown Magnet')
                
                if not nzo_id:
                     # Fallback if hash missing (shouldn't happen for magnets usually)
                     nzo_id = str(uuid.uuid4())[:8]

                conn = get_db_connection()
                try:
                    conn.execute("INSERT OR REPLACE INTO queue (nzo_id, torbox_id, filename, category, status, size, progress, added_at, is_paused) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)",
                              (nzo_id, tid, name, category, 'Queued', time.time(), 1 if paused else 0))
                    conn.commit()
                    added_hashes.append(nzo_id)
                    logger.info(f"Added Torrent (Magnet): {name} [{nzo_id}] - Cat: {category}")
                except Exception as e:
                    logger.error(f"DB Error inserting magnet: {e}")
                finally:
                    conn.close()
            else:
                logger.error(f"Failed to add magnet: {error}")

    # 2. Handle Torrent Files
    if 'torrents' in request.files:
        files = request.files.getlist('torrents')
        for f in files:
            if not f or not f.filename: continue
            
            # Read file content
            content = f.read() # Read into memory
            
            # Extract Hash from torrent file locally (optional but good for consistency)?
            # Or just rely on TorBox response
            
            # Reset file pointer for uploading? Not needed if we pass bytes or if read() consumed
            # 'f' is a FileStorage, we passed content. If we pass f directly, requests reads it. 
            f.seek(0)
            
            data, error = torbox_add_torrent(file_obj=f, filename=f.filename)
            
            if data:
                nzo_id = data.get('hash')
                tid = str(data.get('torrent_id'))
                name = data.get('name', f.filename)
                
                if not nzo_id: nzo_id = str(uuid.uuid4())[:8]
                
                conn = get_db_connection()
                try:
                    conn.execute("INSERT OR REPLACE INTO queue (nzo_id, torbox_id, filename, category, status, size, progress, added_at, is_paused) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)",
                              (nzo_id, tid, name, category, 'Queued', time.time(), 1 if paused else 0))
                    conn.commit()
                    added_hashes.append(nzo_id)
                    logger.info(f"Added Torrent (File): {name} [{nzo_id}] - Cat: {category}")
                except Exception as e:
                    logger.error(f"DB Error inserting torrent file: {e}")
                finally:
                    conn.close()
            else:
                 logger.error(f"Failed to add torrent file {f.filename}: {error}")
                 
    if added_hashes:
        return 'Ok.'
    return 'Fails.', 400

@app.route('/api/v2/torrents/delete', methods=['GET', 'POST'])
def api_v2_torrents_delete():
    hashes = request.values.get('hashes', '')
    delete_files = request.values.get('deleteFiles', 'false') == 'true'
    
    if not hashes: return 'Ok.' # No-op
    
    ids = hashes.split('|')
    conn = get_db_connection()
    try:
        for nzo_id in ids:
            row = conn.execute("SELECT torbox_id, path FROM queue WHERE nzo_id=?", (nzo_id,)).fetchone()
            if row:
                hash_id = nzo_id
                tid = row['torbox_id']
                
                # Stop active download
                if hash_id in ACTIVE_DOWNLOADS:
                    try: ACTIVE_DOWNLOADS[hash_id].terminate()
                    except: pass
                    del ACTIVE_DOWNLOADS[hash_id]
                
                # Delete from Cloud
                # Try both endpoints since we assume torrents but could be mixed use
                torbox_control(tid, 'delete')
                
                # Delete Local Files if requested
                if delete_files and row['path'] and os.path.exists(row['path']):
                    try: shutil.rmtree(row['path'])
                    except: pass
                    
                conn.execute("DELETE FROM queue WHERE nzo_id=?", (hash_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"Delete Error: {e}")
    finally:
        conn.close()
        
    return 'Ok.'

@app.route('/api/v2/torrents/pause', methods=['GET', 'POST'])
def api_v2_torrents_pause():
    hashes = request.values.get('hashes', '')
    if not hashes: return 'Ok.'
    ids = hashes.split('|')
    
    conn = get_db_connection()
    try:
        for nzo_id in ids:
            conn.execute("UPDATE queue SET is_paused=1 WHERE nzo_id=?", (nzo_id,))
            if nzo_id in ACTIVE_DOWNLOADS:
                try: ACTIVE_DOWNLOADS[nzo_id].terminate()
                except: pass
        conn.commit()
    finally:
        conn.close()
    return 'Ok.'

@app.route('/api/v2/torrents/resume', methods=['GET', 'POST'])
def api_v2_torrents_resume():
    hashes = request.values.get('hashes', '')
    if not hashes: return 'Ok.'
    ids = hashes.split('|')
    
    conn = get_db_connection()
    try:
        for nzo_id in ids:
            # Only resume if we have the item
            conn.execute("UPDATE queue SET is_paused=0 WHERE nzo_id=?", (nzo_id,))
            # Reset status if it was Paused_Local to allow retry
            conn.execute("UPDATE queue SET status='Cloud_Done' WHERE nzo_id=? AND status='Paused_Local'", (nzo_id,))
        conn.commit()
    finally:
        conn.close()
    return 'Ok.'

@app.route('/api/v2/torrents/info', methods=['GET', 'POST'])
def api_v2_torrents_info():
    category = request.values.get('category')
    filter_state = request.values.get('filter')
    hashes = request.values.get('hashes')
    
    conn = get_db_connection()
    try:
        query = "SELECT * FROM queue WHERE 1=1"
        params = []
        
        if category:
            query += " AND category = ?"
            params.append(category)
        
        if hashes:
            hash_list = hashes.split('|')
            placeholders = ','.join(['?'] * len(hash_list))
            query += f" AND nzo_id IN ({placeholders})"
            params.extend(hash_list)
            
        rows = conn.execute(query, params).fetchall()
        
        torrents = []
        for r in rows:
            # Map DB state to qBit state
            # qBit states: error, missingFiles, uploading, pausedUP, queuedUP, stalledUP, checkingUP, forcedUP, allocating, downloading, metaDL, pausedDL, queuedDL, stalledDL, checkingDL, forcedDL, checkingResumeData, moving, unknown
            state = 'downloading'
            status = r['status']
            
            if status == 'Completed': state = 'pausedUP' # Completed download -> Seeding/Paused
            elif status == 'Importing': state = 'pausedUP' # Waiting for import (Seeding)
            elif status == 'Paused_Local' or r['is_paused']: state = 'pausedDL'
            elif status == 'Queued': state = 'queuedDL' # Cloud Queued
            elif status == 'Cloud_Done': state = 'stalledDL' # Waiting for transfer slot
            elif status == 'Failed': state = 'error'
            elif status == 'Downloading_Local': state = 'downloading'
            elif status == 'Downloading': state = 'metaDL' # Cloud downloading
            
            progress = (r['progress'] or 0) / 100.0
            total_size = r['size'] or 0
            
            torrents.append({
                'hash': r['nzo_id'],
                'name': r['filename'],
                'size': total_size,
                'total_size': total_size,
                'progress': progress,
                'dlspeed': int(r['speed'] or 0),
                'upspeed': 0,
                'priority': 0,
                'num_seeds': 0,
                'num_leechs': 0,
                'num_incomplete': 0,
                'ratio': 0,
                'eta': int(r['eta'] or 0),
                'state': state,
                'seq_dl': False,
                'f_l_piece_prio': False,
                'category': r['category'],
                'super_seeding': False,
                'force_start': False,
                'save_path': str(r['path']) if r['path'] else f"/downloads/{r['category']}",
                'added_on': int(r['added_at']),
                'completion_on': int(r['last_updated']) if status == 'Completed' else None,
                'magnet_uri': f"magnet:?xt=urn:btih:{r['nzo_id']}&dn={r['filename']}", # Fake magnet for consistency
            })
            
        return jsonify(torrents)
    finally:
        conn.close()



@app.route('/ui/item/<nzo_id>/resume', methods=['POST'])
@login_required
def ui_resume_item(nzo_id):
    conn = get_db_connection()
    try:
        conn.execute("UPDATE queue SET is_paused=0 WHERE nzo_id=?", (nzo_id,))
        # If it was paused during local download, reset to Cloud_Done so it restarts
        conn.execute("UPDATE queue SET status='Cloud_Done' WHERE nzo_id=? AND status='Paused_Local'", (nzo_id,))
        conn.commit()
        return jsonify({"status": True})
    finally:
        conn.close()

@app.route('/ui/item/<nzo_id>/retry', methods=['POST'])
@login_required
def ui_retry_item(nzo_id):
    conn = get_db_connection()
    try:
        conn.execute("UPDATE queue SET status='Queued', failure_reason=NULL, progress=0, is_paused=0 WHERE nzo_id=?", (nzo_id,))
        conn.commit()
        return jsonify({"status": True})
    finally:
        conn.close()

@app.route('/ui/settings')
@login_required
def ui_settings_page():
    conn = get_db_connection()
    try:
        settings = {
            "torbox_api_key": get_setting(conn, "torbox_api_key", ""),
            "sab_api_key": get_setting(conn, "sab_api_key", "torbox123"),
            "max_concurrent": get_setting(conn, "max_concurrent", 3),
            "stuck_timeout": get_setting(conn, "stuck_timeout", 3600),
            "discord_webhook": get_setting(conn, "discord_webhook", "")
        }
        return render_template('settings.html', settings=settings, setup_needed=(not settings['torbox_api_key']))
    finally:
        conn.close()

@app.route('/ui/settings/data')
@login_required
def ui_settings_data():
    conn = get_db_connection()
    try:
        settings = {
            "speed_limit": get_setting(conn, "speed_limit", 0),
            "torbox_api_key": get_setting(conn, "torbox_api_key", os.getenv("TORBOX_API_KEY", "")),
            "sab_api_key": get_setting(conn, "sab_api_key", os.getenv("SABNZBD_API_KEY", "torbox123")),
            "max_concurrent": get_setting(conn, "max_concurrent", 3),
            "stuck_timeout": get_setting(conn, "stuck_timeout", 3600),
            "discord_webhook": get_setting(conn, "discord_webhook", "")
        }
        return jsonify(settings)
    finally:
        conn.close()

@app.route('/ui/settings/save', methods=['POST'])
@login_required
def ui_settings_save():
    data = request.json
    conn = get_db_connection()
    try:
        if 'speed_limit' in data:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('speed_limit', ?)", (str(data['speed_limit']),))
        if 'torbox_api_key' in data:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('torbox_api_key', ?)", (str(data['torbox_api_key']),))
        if 'sab_api_key' in data:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('sab_api_key', ?)", (str(data['sab_api_key']),))
        if 'max_concurrent' in data:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('max_concurrent', ?)", (str(data['max_concurrent']),))
        if 'stuck_timeout' in data:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('stuck_timeout', ?)", (str(data['stuck_timeout']),))
        if 'discord_webhook' in data:
            conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('discord_webhook', ?)", (str(data['discord_webhook']),))
            
        conn.commit()
        
        # Apply Speed Limit immediately via restart loop if needed
        # Other settings generally picked up on next loop or next request, except GLOBALS
        global API_KEY, SAB_API_KEY, MAX_CONCURRENT_DL, STUCK_TIMEOUT
        
        # Update Globals immediately where possible
        if 'torbox_api_key' in data: API_KEY = data['torbox_api_key'] 
        if 'sab_api_key' in data: SAB_API_KEY = data['sab_api_key']
        if 'max_concurrent' in data: MAX_CONCURRENT_DL = int(data['max_concurrent'])
        if 'stuck_timeout' in data: STUCK_TIMEOUT = int(data['stuck_timeout'])
        
        # Determine if we should restart active downloads
        # If speed limit changed, we rely on the threads to poll DB, but we probably need to force them if we are moving from 0 so they actually start using the limit
        # The new thread logic polls DB every 2 seconds for speed_limit
        
        return jsonify({"status": "saved"})
    finally:
        conn.close()

@app.route('/ui/settings/speed_limit', methods=['POST'])
@login_required
def ui_set_speed_limit():
    try:
        data = request.json
        limit = float(data.get('limit', 0))
        conn = get_db_connection()
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('speed_limit', ?)", (str(limit),))
        conn.commit()
        conn.close()
        
        return jsonify({"status": "saved", "message": "Speed limit saved."})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/ui/clean_downloads', methods=['POST'])
@login_required
def ui_clean_downloads():
    try:
        logging.info("Starting cleanup of downloads folder...")
        # 1. Stop all active downloads first
        global ACTIVE_DOWNLOADS
        for nzo_id, proc in list(ACTIVE_DOWNLOADS.items()):
            try:
                proc.terminate()
                logging.info(f"Terminated download {nzo_id} for cleanup")
            except: pass
        ACTIVE_DOWNLOADS.clear()

        # 2. Wipe /downloads folder contents
        base_dir = '/downloads'
        if os.path.exists(base_dir):
            for item in os.listdir(base_dir):
                item_path = os.path.join(base_dir, item)
                try:
                    if os.path.isfile(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as e:
                    logging.error(f"Failed to delete {item_path}: {e}")
        
        # 3. Recreate structure
        folders = ['completed', 'incomplete', 'movies', 'tv', 'temp', 'watch']
        for f in folders:
            os.makedirs(os.path.join(base_dir, f), exist_ok=True)
            
        return jsonify({"status": True})
    except Exception as e:
        logging.error(f"Cleanup failed: {e}")
        return jsonify({"status": False, "error": str(e)})

@app.route('/ui/queue/<action>/<nzo_id>', methods=['POST'])
@login_required
def ui_queue_reorder(action, nzo_id):
    conn = get_db_connection()
    try:
        # Simple Priority Logic:
        # 'top': Set priority = max(priority) + 1
        # 'bottom': Set priority = min(priority) - 1
        # 'up': Increase priority by 1
        # 'down': Decrease priority by 1
        
        row = conn.execute("SELECT priority FROM queue WHERE nzo_id=?", (nzo_id,)).fetchone()
        if not row: return jsonify({"status": False, "error": "Item not found"})
        
        current_priority = row['priority'] or 0
        new_priority = current_priority
        
        if action == 'top':
            cur_max = conn.execute("SELECT MAX(priority) FROM queue").fetchone()[0] or 0
            new_priority = cur_max + 1
        elif action == 'bottom':
            cur_min = conn.execute("SELECT MIN(priority) FROM queue").fetchone()[0] or 0
            new_priority = cur_min - 1
        elif action == 'up':
            new_priority = current_priority + 1
        elif action == 'down':
            new_priority = current_priority - 1
            
        conn.execute("UPDATE queue SET priority=? WHERE nzo_id=?", (new_priority, nzo_id))
        conn.commit()
        return jsonify({"status": True})
    except Exception as e:
        return jsonify({"status": False, "error": str(e)})
    finally:
        conn.close()

@app.route('/ui/reset_cache', methods=['POST'])
@login_required
def ui_reset_cache():
    conn = get_db_connection()
    try:
        logging.info("Starting full cache reset...")
        # 1. Stop all active downloads
        global ACTIVE_DOWNLOADS
        for nzo_id, proc in list(ACTIVE_DOWNLOADS.items()):
            try:
                proc.terminate()
            except: pass
        ACTIVE_DOWNLOADS.clear()

        # 2. Clear Queue Database (Keep Settings)
        # We only delete operational data, not configuration
        conn.execute("DELETE FROM queue")
        # conn.execute("DELETE FROM history") # Table does not exist
        conn.commit()
        
        # 3. Trigger restart to reload fresh state
        # Restarting ensures in-memory state is also cleared
        def _restart_app():
            time.sleep(1)
            python = sys.executable
            os.execl(python, python, *sys.argv)
            
        threading.Thread(target=_restart_app).start()
        
        return jsonify({"status": True})
    except Exception as e:
        logging.error(f"Reset failed: {e}")
        # Even if DB clear fails, we might still want to restart?
        return jsonify({"status": False, "error": str(e)})
    finally:
        conn.close()

@app.route('/sabnzbd/api', methods=['GET', 'POST'])
@app.route('/api', methods=['GET', 'POST'])
def sab_api():
    mode = request.values.get('mode', '')
    apikey = request.values.get('apikey')
    
    # Verbose logging of ALL incoming API requests from Sonarr/Radarr
    if DEBUG_MODE:
        args = {k:v for k,v in request.values.items() if k != 'apikey'} # Don't log api key
        
        # Filter out routine polling logs to reduce noise
        quiet_modes = ['queue', 'history', 'version', 'get_config', 'fullstatus']
        if mode in quiet_modes and 'name' not in args and 'search' not in args:
             pass # Skip logging routine checks
        else:
             logger.debug(f"API Request: mode={mode} args={args}")

    if apikey != SAB_API_KEY:
        if DEBUG_MODE: logger.warning(f"API Key mismatch. Got: {apikey}")
        return jsonify({"error": "Invalid API Key"}), 403

    conn = get_db_connection()
    
    if mode == 'version':
        return jsonify({"version": "3.7.2"}) 

    if mode == 'get_config':
        return jsonify({
            "config": {
                "misc": {"complete_dir": "/downloads"},
                "categories": [
                    {"name": "*", "dir": ""},
                    {"name": "tv", "dir": "tv"},
                    {"name": "movies", "dir": "movies"},
                    {"name": "default", "dir": "default"}
                ]
            }
        })

    if mode == 'queue':
        # --- DELETE HANDLER (Moved for Sonarr compatibility) ---
        if request.values.get('name') == 'delete':
            value = request.values.get('value')
            if value:
                # Handle single or comma-separated list
                ids = value.split(',')
                for nzo_id in ids:
                    row = conn.execute("SELECT torbox_id, status, path FROM queue WHERE nzo_id=?", (nzo_id,)).fetchone()
                    if row:
                        # Kill active local download if running
                        if nzo_id in ACTIVE_DOWNLOADS:
                            try:
                                ACTIVE_DOWNLOADS[nzo_id].terminate()
                                logging.info(f"Cancelled active download {nzo_id}")
                                del ACTIVE_DOWNLOADS[nzo_id]
                            except Exception as e:
                                logging.error(f"Failed to kill process {nzo_id}: {e}")

                        # If local downloading thread is active, it won't be killed immediately
                        # but setting status to Failed prevents further actions?
                        # Actually we should try to kill aria2 if we could track PID, 
                        # but simple DB removal + TorBox delete is usually enough.
                        torbox_control(row['torbox_id'], 'delete')
                        conn.execute("DELETE FROM queue WHERE nzo_id=?", (nzo_id,))
                conn.commit()
                return jsonify({"status": True, "nzo_ids": ids})
            return jsonify({"status": False})

        # --- LISTING HANDLER ---
        rows = conn.execute("SELECT * FROM queue WHERE status NOT IN ('Completed', 'Failed')").fetchall()
        slots = []
        global_speed = 0.0
        
        for r in rows:
            sab_status = "Downloading"
            if r['status'] == 'Queued': sab_status = "Queued"
            if r['status'] == 'Downloading_Local': sab_status = "Downloading"
            
            # Formatting for Sonarr
            total_bytes = r['size'] if r['size'] else 0
            if total_bytes == 0: total_bytes = 1000 * 1024 * 1024 # Default to 1GB if unknown
            
            # Get Speed & ETA
            speed = r['speed'] if r['speed'] else 0
            eta = r['eta'] if r['eta'] else 0
            
            # Calculate dynamic ETA if not provided by Cloud
            progress_fraction = (r['progress'] / 100)
            mb_left = (total_bytes * (1 - progress_fraction)) / (1024 * 1024)
            
            if speed > 0:
                global_speed += speed
                if eta == 0: # Recalculate if 0
                    bytes_left = total_bytes * (1 - progress_fraction)
                    eta = bytes_left / speed

            timeleft_str = format_time(eta)

            slots.append({
                "status": sab_status,
                "index": 0,
                "password": "",
                "avg_age": "10m",
                "script": "None",
                "size": f"{total_bytes / (1024*1024*1024):.2f} GB",
                "sizeleft": f"{mb_left / 1024:.2f} GB",
                "mbleft": f"{mb_left:.2f}",
                "mb": f"{total_bytes / (1024 * 1024):.2f}",
                "msgid": "",
                "priority": "Normal",
                "cat": r['category'],
                "mb_missing": "0",
                "percentage": str(int(r['progress'])),
                "nzo_id": r['nzo_id'],
                "filename": r['filename'],
                "timeleft": timeleft_str,
                "eta": timeleft_str
            })
            
        return jsonify({
            "queue": {
                "status": "Downloading",
                "speed": f"{global_speed / (1024 * 1024):.2f} MB/s",
                "speedlimit": "0",
                "paused": False,
                "slots": slots,
                "limit": 0,
                "start": 0,
                "finish": 0,
                "bytepersec": str(int(global_speed)),
                "size": "100 GB",
                "sizeleft": "50 GB",
                "categories": ["*", "tv", "movies", "default"] 
            }
        })

    if mode == 'history':
        limit = int(request.values.get('limit', 10))
        rows = conn.execute("SELECT * FROM queue WHERE status IN ('Completed', 'Failed') ORDER BY added_at DESC LIMIT ?", (limit,)).fetchall()
        slots = []
        for r in rows:
            slots.append({
                "nzo_id": r['nzo_id'],
                "name": r['filename'],
                "size": "1.0 GB",
                "category": r['category'],
                "pp": "D",
                "script": "None",
                "report": "",
                "url": "",
                "status": "Completed" if r['status'] == 'Completed' else "Failed",
                "nzo_id": r['nzo_id'],
                "storage": r['path'],
                "path": r['path'],
                "script_line": "",
                "completed": int(time.time()),
                "completeness": 100 if r['status'] == 'Completed' else 0,
                "fail_message": r['failure_reason'] or ""
            })
        return jsonify({"history": {"slots": slots}})

    if mode == 'addfile':
        name = request.values.get('name')
        cat = request.values.get('cat', 'default')
        nzb_file = request.files.get('name') or request.files.get('nzbfile')
        
        if (not name or name == 'Unknown') and nzb_file and nzb_file.filename:
            name = secure_filename(nzb_file.filename)
            
        if not name or name == 'Unknown':
             name = f"nzb_upload_{int(time.time())}"

        if nzb_file:
            logger.info(f"Received NZB Upload: {name} (Category: {cat})")
            
            file_content = nzb_file.read()
            
            # Retry logic for adding to TorBox (3 attempts)
            tid = None
            err = "Unknown Error"
            for attempt in range(3):
                if attempt > 0:
                    logger.info(f"Retrying upload for {name} (Attempt {attempt+1}/3)...")
                    time.sleep(2) # Wait 2 seconds before retry
                    
                # Need to reset file pointer if reading multiple times? 
                # Actually file_content is in memory now, so pass bytes
                tid, err = torbox_add_nzb(file_content, name)
                
                if tid:
                    break
            
            if tid:
                logger.info(f"Successfully added to TorBox. ID: {tid}")
                nzo_id = str(uuid.uuid4())[:8]
                c = conn.cursor()
                c.execute("INSERT INTO queue (nzo_id, torbox_id, filename, category, status, size, progress, added_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                          (nzo_id, str(tid), name, cat, 'Queued', 0, 0, time.time()))
                conn.commit()
                return jsonify({"status": True, "nzo_ids": [nzo_id]})
            else:
                logger.error(f"Failed to add to TorBox: {err}")
                return jsonify({"status": False, "nzo_ids": [], "error": err})
        
        return jsonify({"status": False, "nzo_ids": []})
    
    if mode == 'fullstatus':
         return jsonify({"status": {"speed": "0", "paused": False}})

    return jsonify({"error": "Unknown mode"})

if __name__ == "__main__":
    init_db()
    load_config()
    t = threading.Thread(target=download_worker, daemon=True)
    t.start()
    port = int(os.getenv("PORT", 5051))
    app.run(host='0.0.0.0', port=port)