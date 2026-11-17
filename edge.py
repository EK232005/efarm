#!/usr/bin/env python3
"""
eFarm Plant Monitor + AWS IoT/S3 uploader

Local operation:
  - Reads newline-delimited JSON from the ESP32 over Bluetooth Classic.
  - Samples the latest valid reading once per minute.
  - Logs each one-minute sample to data/sensor_data.csv.
  - Captures a real Logitech C270 USB webcam photo daily at 08:00 Asia/Kolkata time.
  - Supports manual photos and ESP32 soil calibration commands.
  - Keeps pending cloud work in a persistent local JSONL outbox.

Cloud operation:
  - Publishes telemetry to efarm/<device_id>/telemetry.
  - Publishes online/offline status to efarm/<device_id>/status.
  - Uploads photos to S3 and announces them on efarm/<device_id>/evt/image.
  - Listens for capture requests on efarm/<device_id>/cmd/capture.

No internet is required for local sensing, CSV logging, calibration, or
photograph capture. Pending telemetry/photo uploads are retried when the
cloud connection and AWS credentials endpoint become available.
"""

import csv
import json
import os
import queue
import socket
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import paho.mqtt.client as mqtt
import requests
import ssl
import yaml


# =========================================================
# CONFIGURATION
# =========================================================

ESP32_MAC = "88:57:21:B2:EF:0A"
RFCOMM_CHANNEL = 1

BASE_DIR = Path.home() / "efarm"
DATA_DIR = BASE_DIR / "data"
PHOTO_DIR = BASE_DIR / "photos"
LOG_DIR = BASE_DIR / "logs"

CSV_FILE = DATA_DIR / "sensor_data.csv"
OUTBOX_FILE = DATA_DIR / "cloud_outbox.jsonl"
LAST_DAILY_PHOTO_FILE = DATA_DIR / "last_daily_photo.txt"
LOG_FILE = LOG_DIR / "efarm.log"

CONFIG_FILE = Path(os.getenv("EFARM_CONFIG", str(BASE_DIR / "device.yaml")))

# ESP32 sends more frequently; the Pi samples the latest value once/minute.
SENSOR_INTERVAL_SECONDS = 60
SENSOR_STALE_AFTER_SECONDS = 15

# Daily photo time in Raspberry Pi local time.
DAILY_PHOTO_HOUR = 8
DAILY_PHOTO_MINUTE = 0
SCHEDULER_INTERVAL_SECONDS = 20
LOCAL_TZ = ZoneInfo("Asia/Kolkata")

# Logitech C270 USB webcam settings. These match the working capture test.
CAMERA_DEVICE = "/dev/video0"
CAMERA_WIDTH = 1280
CAMERA_HEIGHT = 720
CAMERA_FPS = 10

FW_VERSION = "efarm-pi-esp32-1.0"
SCHEMA_VERSION = 1

command_queue = queue.Queue()
cloud_capture_event = threading.Event()
camera_lock = threading.Lock()
outbox_lock = threading.RLock()
running = True

latest_sensor = None
latest_sensor_received_at = None
mqtt_client = None
mqtt_connected = threading.Event()
cloud_config = None
DEVICE = None
T_TELEM = None
T_STATUS = None
T_IMG = None
T_CMD = None


# =========================================================
# LOGGING / DIRECTORIES / CONFIG
# =========================================================

def log(message):
    """Print and append a timestamped message to the local log."""
    timestamp = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_FILE.open("a", encoding="utf-8") as file:
            file.write(line + "\n")
    except OSError:
        pass


def initialize_directories():
    for directory in (DATA_DIR, PHOTO_DIR, LOG_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def load_config():
    global LOCAL_TZ
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_FILE}"
        )

    with CONFIG_FILE.open("r", encoding="utf-8") as file:
        cfg = yaml.safe_load(file) or {}

    LOCAL_TZ = ZoneInfo(cfg.get("timezone", "Asia/Kolkata"))

    required = [
        "device_id", "region", "mqtt_endpoint", "cred_endpoint",
        "role_alias", "bucket", "certs"
    ]
    missing = [key for key in required if key not in cfg]
    if missing:
        raise ValueError(
            "Missing device.yaml fields: " + ", ".join(missing)
        )

    for key in ("ca", "cert", "key"):
        if key not in cfg["certs"]:
            raise ValueError(f"Missing device.yaml field: certs.{key}")
        cert_path = Path(cfg["certs"][key]).expanduser()
        if not cert_path.is_absolute():
            cert_path = (CONFIG_FILE.parent / cert_path).resolve()
        if not cert_path.exists():
            raise FileNotFoundError(f"Certificate file not found: {cert_path}")
        cfg["certs"][key] = str(cert_path)

    return cfg


def now_ms():
    return int(time.time() * 1000)


# =========================================================
# CSV: ONE LOCAL ROW PER MINUTE
# =========================================================

def initialize_csv():
    initialize_directories()
    if not CSV_FILE.exists():
        with CSV_FILE.open("w", newline="", encoding="utf-8") as file:
            csv.writer(file).writerow([
                "timestamp",
                "temperature_C",
                "humidity_percent",
                "soil_raw",
                "soil_percent",
            ])


def log_sensor_sample(data, timestamp):
    try:
        with CSV_FILE.open("a", newline="", encoding="utf-8") as file:
            csv.writer(file).writerow([
                timestamp,
                data.get("temperature"),
                data.get("humidity"),
                data.get("soil_raw"),
                data.get("soil_percent"),
            ])
    except OSError as error:
        log(f"[CSV ERROR] Could not write sensor sample: {error}")


# =========================================================
# PERSISTENT CLOUD OUTBOX
# =========================================================

def enqueue_outbox(item):
    """
    Persist work before attempting cloud delivery.
    Supported entries: {"kind":"mqtt", ...} and {"kind":"photo", ...}
    """
    initialize_directories()
    encoded = json.dumps(item, separators=(",", ":"))

    with outbox_lock:
        with OUTBOX_FILE.open("a", encoding="utf-8") as file:
            file.write(encoded + "\n")


def read_outbox():
    if not OUTBOX_FILE.exists():
        return []

    items = []
    with outbox_lock:
        with OUTBOX_FILE.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError:
                    log("[OUTBOX WARNING] Skipping malformed outbox line.")
    return items


def rewrite_outbox(items):
    temporary = OUTBOX_FILE.with_suffix(".tmp")
    with outbox_lock:
        with temporary.open("w", encoding="utf-8") as file:
            for item in items:
                file.write(json.dumps(item, separators=(",", ":")) + "\n")
        temporary.replace(OUTBOX_FILE)


# =========================================================
# CAMERA
# =========================================================

def make_photo_filename(now):
    # Example: monday_15Nov26_8am.jpg
    day = now.strftime("%A").lower()
    date = now.strftime("%d%b%y")
    hour = now.strftime("%I%p").lstrip("0").lower()
    return f"{day}{date}{hour}.jpg"



def capture_photo(source="manual"):
    """Capture a Logitech C270 image after camera warm-up and exposure stabilization."""
    initialize_directories()

    now = datetime.now(LOCAL_TZ)
    photo_path = PHOTO_DIR / make_photo_filename(now)

    # Never overwrite an existing photograph.
    counter = 1
    while photo_path.exists():
        photo_path = PHOTO_DIR / (
            f"{Path(make_photo_filename(now)).stem}_{counter:02d}.jpg"
        )
        counter += 1

    command = [
        "fswebcam",
        "--device", CAMERA_DEVICE,
        "--resolution", f"{CAMERA_WIDTH}x{CAMERA_HEIGHT}",
        "--no-banner",
        "--delay", "3",
        "--skip", "60",
        str(photo_path),
    ]

    with camera_lock:
        try:
            log(f"[CAMERA] Initializing camera for {source} capture...")
            log("[CAMERA] Waiting for exposure stabilization...")

            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=40,
                check=False,
            )

            if (
                result.returncode == 0
                and photo_path.exists()
                and photo_path.stat().st_size > 10_000
            ):
                log(
                    f"[CAMERA] Photograph captured successfully: "
                    f"{photo_path} ({photo_path.stat().st_size} bytes)"
                )
                return photo_path

            log("[CAMERA ERROR] Invalid or empty camera image.")

            if result.stderr:
                log(result.stderr[-1500:])

            if photo_path.exists():
                photo_path.unlink()

        except subprocess.TimeoutExpired:
            log("[CAMERA ERROR] Camera capture timed out.")

        except FileNotFoundError:
            log("[CAMERA ERROR] fswebcam is not installed.")

        except Exception as error:
            log(f"[CAMERA ERROR] {type(error).__name__}: {error}")

    return None



def queue_photo_for_cloud(photo_path, trigger):
    enqueue_outbox({
        "kind": "photo",
        "path": str(photo_path),
        "trigger": trigger,
    })
    log(f"[CLOUD] Photo queued for upload: {photo_path.name}")


def daily_photo_scheduler():
    """
    At/after 08:00, take one scheduled photo per local calendar day.
    If the app starts after 08:00 and today's photo is missing, take it
    immediately. A successful local capture is queued for cloud upload.
    """
    initialize_directories()

    while running:
        now = datetime.now(LOCAL_TZ)
        today = now.strftime("%Y-%m-%d")

        try:
            last_date = (
                LAST_DAILY_PHOTO_FILE.read_text(encoding="utf-8").strip()
                if LAST_DAILY_PHOTO_FILE.exists()
                else ""
            )
        except OSError as error:
            log(f"[CAMERA] Could not read daily state: {error}")
            last_date = ""

        due = (
            now.hour > DAILY_PHOTO_HOUR
            or (
                now.hour == DAILY_PHOTO_HOUR
                and now.minute >= DAILY_PHOTO_MINUTE
            )
        )

        if due and last_date != today:
            log("[CAMERA] Daily photograph is due.")
            photo_path = capture_photo(source="daily")

            if photo_path:
                try:
                    # Persist upload task before marking today's capture done.
                    queue_photo_for_cloud(photo_path, "scheduled")
                    LAST_DAILY_PHOTO_FILE.write_text(
                        today, encoding="utf-8"
                    )
                    log("[CAMERA] Daily capture completed and queued.")
                except OSError as error:
                    log(f"[CAMERA ERROR] Could not persist photo state: {error}")

        time.sleep(SCHEDULER_INTERVAL_SECONDS)


def start_camera_scheduler():
    threading.Thread(
        target=daily_photo_scheduler,
        name="daily-photo-scheduler",
        daemon=True,
    ).start()
    log("[CAMERA] Daily scheduler active for 08:00 local time.")


# =========================================================
# AWS IoT MQTT
# =========================================================

def mqtt_on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        mqtt_connected.clear()
        log(f"[MQTT] Connection rejected: {reason_code}")
        return

    mqtt_connected.set()
    log("[MQTT] Connected to AWS IoT Core.")

    result, mid = client.subscribe(T_CMD, qos=1)
    if result == mqtt.MQTT_ERR_SUCCESS:
        log(f"[MQTT] Subscribed to {T_CMD}")
    else:
        log(f"[MQTT] Subscribe request failed: {result}")

    # Queue online status; the normal outbox flusher publishes it.
    try:
        enqueue_outbox({
            "kind": "mqtt",
            "topic": T_STATUS,
            "payload": {
                "schema_version": SCHEMA_VERSION,
                "device_id": DEVICE,
                "state": "online",
                "ts": now_ms(),
                "fw": FW_VERSION,
            },
        })
    except OSError as error:
        log(f"[OUTBOX ERROR] Could not queue online status: {error}")


def mqtt_on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    mqtt_connected.clear()
    log(f"[MQTT] Disconnected: {reason_code}. Will reconnect when possible.")


def mqtt_on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
        log(f"[MQTT] Command received: {payload}")

        # Support the connection-test capture command and a simple command.
        command = payload.get("cmd") or payload.get("command")
        if command in ("capture", "capture_photo", "take_photo") or payload.get("capture") is True:
            cloud_capture_event.set()
    except Exception as error:
        log(f"[MQTT] Invalid command payload: {error}")


def start_mqtt(cfg):
    global mqtt_client

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=DEVICE,
    )

    client.tls_set(
        ca_certs=cfg["certs"]["ca"],
        certfile=cfg["certs"]["cert"],
        keyfile=cfg["certs"]["key"],
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )

    client.will_set(
        T_STATUS,
        json.dumps({
            "schema_version": SCHEMA_VERSION,
            "device_id": DEVICE,
            "state": "offline",
            "ts": now_ms(),
            "fw": FW_VERSION,
        }),
        qos=1,
        retain=False,
    )

    client.on_connect = mqtt_on_connect
    client.on_disconnect = mqtt_on_disconnect
    client.on_message = mqtt_on_message

    mqtt_client = client

    # connect_async + loop_start allows local monitoring to continue
    # even when the network or AWS IoT endpoint is unavailable.
    client.reconnect_delay_set(min_delay=2, max_delay=60)
    client.connect_async(cfg["mqtt_endpoint"], 8883, keepalive=30)
    client.loop_start()
    log("[MQTT] Background connection/reconnection started.")


def mqtt_publish(topic, payload):
    if mqtt_client is None or not mqtt_connected.is_set():
        return False

    try:
        info = mqtt_client.publish(
            topic,
            json.dumps(payload, separators=(",", ":")),
            qos=1,
            retain=False,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return False
        info.wait_for_publish(timeout=10)
        return info.is_published()
    except Exception as error:
        log(f"[MQTT] Publish error: {error}")
        return False


# =========================================================
# AWS TEMPORARY CREDENTIALS + S3 PHOTO UPLOAD
# =========================================================

def get_s3_client():
    """Get temporary AWS credentials from the IoT credentials provider."""
    url = (
        f"https://{cloud_config['cred_endpoint']}"
        f"/role-aliases/{cloud_config['role_alias']}/credentials"
    )

    response = requests.get(
        url,
        headers={"x-amzn-iot-thingname": DEVICE},
        cert=(
            cloud_config["certs"]["cert"],
            cloud_config["certs"]["key"],
        ),
        timeout=20,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Credentials endpoint HTTP {response.status_code}: "
            f"{response.text[:300]}"
        )

    credentials = response.json()["credentials"]

    return boto3.client(
        "s3",
        region_name=cloud_config["region"],
        aws_access_key_id=credentials["accessKeyId"],
        aws_secret_access_key=credentials["secretAccessKey"],
        aws_session_token=credentials["sessionToken"],
    )


def upload_photo_and_announce(photo_path, trigger):
    photo_path = Path(photo_path)

    if not photo_path.exists():
        raise FileNotFoundError(f"Photo no longer exists: {photo_path}")

    s3 = get_s3_client()
    timestamp = int(photo_path.stat().st_mtime * 1000)
    key = f"images/{DEVICE}/{photo_path.name}"

    with photo_path.open("rb") as file:
        s3.put_object(
            Bucket=cloud_config["bucket"],
            Key=key,
            Body=file,
            ContentType="image/jpeg",
        )

    event = {
        "schema_version": SCHEMA_VERSION,
        "device_id": DEVICE,
        "ts": timestamp,
        "s3_key": key,
        "trigger": trigger,
    }

    if not mqtt_publish(T_IMG, event):
        # Return the event to the outbox flusher instead of appending it.
        # This prevents the flusher's subsequent rewrite from losing it.
        log(f"[CLOUD] S3 upload succeeded; image event still needs publishing: {key}")
        return {
            "kind": "mqtt",
            "topic": T_IMG,
            "payload": event,
        }

    log(f"[CLOUD] Photo uploaded and announced: s3://{cloud_config['bucket']}/{key}")
    return True


# =========================================================
# OUTBOX DELIVERY
# =========================================================

def deliver_outbox_item(item):
    kind = item.get("kind")

    if kind == "mqtt":
        return mqtt_publish(item["topic"], item["payload"])

    if kind == "photo":
        return upload_photo_and_announce(
            item["path"], item.get("trigger", "scheduled")
        )

    log(f"[OUTBOX] Unknown item type: {kind}")
    return True


def flush_outbox(max_items=10):
    """
    Deliver queued work in order. Stop at the first failure and preserve
    that item plus everything after it for a later retry.
    """
    if cloud_config is None or not mqtt_connected.is_set():
        return

    # Hold the re-entrant lock for the whole read/deliver/rewrite cycle.
    # This prevents a background photo thread from appending an item just
    # before the outbox is rewritten and accidentally losing that new item.
    with outbox_lock:
        items = read_outbox()
        if not items:
            return

        remaining = items[:]
        delivered = 0

        changed = False
        for item in items[:max_items]:
            try:
                result = deliver_outbox_item(item)
                if result is False:
                    break

                if isinstance(result, dict):
                    # The S3 photo upload succeeded but its MQTT event did
                    # not. Replace the photo task with the event task.
                    remaining[0] = result
                    changed = True
                    break

                remaining.pop(0)
                delivered += 1
                changed = True
            except Exception as error:
                log(f"[OUTBOX] Delivery failed; will retry: {error}")
                break

        if changed:
            rewrite_outbox(remaining)
            log(
                f"[OUTBOX] Completed {delivered} queued item(s); "
                f"{len(remaining)} remain."
            )


# =========================================================
# SENSOR PAYLOAD: ONE SAMPLE PER MINUTE
# =========================================================

def make_telemetry_payload(data):
    return {
        "schema_version": SCHEMA_VERSION,
        "device_id": DEVICE,
        "ts": now_ms(),
        "readings": {
            "air_temp_c": data.get("temperature"),
            "air_humidity_pct": data.get("humidity"),
            "soil_moisture_raw": data.get("soil_raw"),
            "soil_moisture_pct": data.get("soil_percent"),
        },
        "fw": FW_VERSION,
    }


def sample_and_queue_sensor():
    """Log and queue the latest fresh sensor reading."""
    if latest_sensor is None or latest_sensor_received_at is None:
        log("[SENSOR] No ESP32 reading received yet; minute sample skipped.")
        return

    age = time.monotonic() - latest_sensor_received_at
    if age > SENSOR_STALE_AFTER_SECONDS:
        log(f"[SENSOR] Latest ESP32 reading is stale ({age:.1f}s); sample skipped.")
        return

    data = dict(latest_sensor)
    timestamp = datetime.now(LOCAL_TZ).isoformat(timespec="seconds")

    log_sensor_sample(data, timestamp)

    payload = make_telemetry_payload(data)

    if T_TELEM is None:
        log(
            "[SENSOR] One-minute sample logged locally; cloud is not "
            "configured, so telemetry was not queued."
        )
        return

    try:
        enqueue_outbox({
            "kind": "mqtt",
            "topic": T_TELEM,
            "payload": payload,
        })
        log(
            "[SENSOR] One-minute sample logged and queued: "
            f"T={data.get('temperature')} C, "
            f"RH={data.get('humidity')} %, "
            f"Soil={data.get('soil_percent')} %"
        )
    except OSError as error:
        log(f"[OUTBOX ERROR] Could not queue telemetry: {error}")


# =========================================================
# BLUETOOTH ESP32 CONNECTION
# =========================================================

def connect_esp32():
    log("[BT] Connecting to ESP32...")

    sock = socket.socket(
        socket.AF_BLUETOOTH,
        socket.SOCK_STREAM,
        socket.BTPROTO_RFCOMM,
    )
    sock.settimeout(10)

    try:
        sock.connect((ESP32_MAC, RFCOMM_CHANNEL))
        sock.settimeout(0.5)
        log("[BT] Connected to PlantMonitor.")
        return sock
    except Exception:
        sock.close()
        raise


def send_esp32_command(sock, command):
    message = json.dumps(command) + "\n"
    sock.sendall(message.encode("utf-8"))
    log("[BT TX] " + message.strip())


def process_esp32_message(message):
    global latest_sensor, latest_sensor_received_at

    try:
        data = json.loads(message)
        msg_type = data.get("type")

        if msg_type == "sensor":
            latest_sensor = data
            latest_sensor_received_at = time.monotonic()

        elif msg_type == "calibration":
            log("[CALIBRATION] " + json.dumps(data))

        elif msg_type == "calibration_status":
            log("[CALIBRATION STATUS] " + json.dumps(data))

        elif msg_type == "ack":
            log("[ESP32 ACK] " + json.dumps(data))

        else:
            log("[ESP32] " + json.dumps(data))

    except (json.JSONDecodeError, TypeError) as error:
        log(f"[BT ERROR] Invalid ESP32 message {message!r}: {error}")


# =========================================================
# TERMINAL COMMANDS
# =========================================================

def command_input():
    global running

    print("""
========== EFARM CONTROL ==========
  help                Show commands
  photo               Capture and queue a photo now
  dry                 Calibrate dry soil
  wet                 Calibrate wet soil
  status              Get ESP32 calibration settings
  offset -1 -5        Set temperature/humidity offsets
  cloud               Show cloud connection/outbox status
  quit                Exit
===================================
""")

    while running:
        try:
            command = input("eFarm> ").strip()
            if command:
                command_queue.put(command)
            if command.lower() == "quit":
                break
        except EOFError:
            command_queue.put("quit")
            break


def process_terminal_command(command, sock):
    global running

    parts = command.split()
    if not parts:
        return

    cmd = parts[0].lower()

    if cmd == "quit":
        running = False
        return

    if cmd == "photo":
        photo_path = capture_photo(source="manual")
        if photo_path:
            try:
                queue_photo_for_cloud(photo_path, "on_demand")
            except OSError as error:
                log(f"[OUTBOX ERROR] Photo saved locally but could not queue upload: {error}")
        return

    if cmd == "cloud":
        log(
            f"[CLOUD] configured={cloud_config is not None}, "
            f"MQTT connected={mqtt_connected.is_set()}, "
            f"pending outbox items={len(read_outbox())}"
        )
        return

    if cmd == "help":
        print("Commands: photo, dry, wet, status, offset TEMP HUMIDITY, cloud, quit")
        return

    if sock is None:
        log("[BT] ESP32 offline; calibration command not sent.")
        return

    try:
        if cmd == "dry":
            print(
                "Prepare an air-dried soil sample, insert the sensor at "
                "the normal depth, keep it still, then wait for calibration."
            )
            send_esp32_command(sock, {"cmd": "soil_calibrate_dry"})

        elif cmd == "wet":
            print(
                "Thoroughly wet the soil sample, let excess water drain, "
                "insert at the same depth, and keep the sensor still."
            )
            send_esp32_command(sock, {"cmd": "soil_calibrate_wet"})

        elif cmd == "status":
            send_esp32_command(sock, {"cmd": "get_calibration"})

        elif cmd == "offset":
            if len(parts) != 3:
                log("Usage: offset TEMP_OFFSET HUMIDITY_OFFSET")
                return

            send_esp32_command(sock, {
                "cmd": "set_offsets",
                "temperature_offset": float(parts[1]),
                "humidity_offset": float(parts[2]),
            })

        else:
            log("Unknown command. Type help.")

    except ValueError:
        log("[ERROR] Offset values must be numeric.")
    except OSError as error:
        log(f"[BT ERROR] Could not send command: {error}")


# =========================================================
# MAIN
# =========================================================

def main():
    global running, cloud_config, DEVICE, T_TELEM, T_STATUS, T_IMG, T_CMD

    initialize_directories()
    initialize_csv()

    try:
        cloud_config = load_config()
        DEVICE = cloud_config["device_id"]

        T_TELEM = f"efarm/{DEVICE}/telemetry"
        T_STATUS = f"efarm/{DEVICE}/status"
        T_IMG = f"efarm/{DEVICE}/evt/image"
        T_CMD = f"efarm/{DEVICE}/cmd/capture"
    except Exception as error:
        cloud_config = None
        DEVICE = None
        T_TELEM = T_STATUS = T_IMG = T_CMD = None
        log(
            f"[CLOUD] Cloud configuration unavailable: {error}. "
            "Continuing in local-only mode."
        )

    log("========================================")
    log("       eFarm Plant Monitor")
    log("========================================")
    log(f"[SYSTEM] Device ID: {DEVICE or 'not configured'}")
    log(f"[SYSTEM] Sensor interval: {SENSOR_INTERVAL_SECONDS}s")
    log(f"[SYSTEM] Daily photo: {DAILY_PHOTO_HOUR:02d}:{DAILY_PHOTO_MINUTE:02d}")
    log(f"[SYSTEM] Local CSV: {CSV_FILE}")
    log(f"[SYSTEM] Local photos: {PHOTO_DIR}")
    log(f"[SYSTEM] Cloud outbox: {OUTBOX_FILE}")

    # MQTT runs independently; missing config or network does not stop local work.
    if cloud_config is not None:
        try:
            start_mqtt(cloud_config)
        except Exception as error:
            log(f"[MQTT ERROR] Could not start MQTT client: {error}")

    start_camera_scheduler()

    threading.Thread(
        target=command_input,
        name="terminal-command-input",
        daemon=True,
    ).start()

    sock = None
    buffer = ""
    next_sensor_sample = time.monotonic() + SENSOR_INTERVAL_SECONDS
    next_outbox_flush = time.monotonic() + 5
    next_bt_attempt = 0

    try:
        while running:
            now_mono = time.monotonic()

            # Connect/reconnect Bluetooth without blocking local/cloud tasks
            # for long periods. connect_esp32 itself has a 10-second timeout.
            if sock is None and now_mono >= next_bt_attempt:
                try:
                    sock = connect_esp32()
                    buffer = ""
                except OSError as error:
                    log(f"[BT] Connection failed: {error}")
                    next_bt_attempt = time.monotonic() + 5

            # Terminal commands work whether Bluetooth is connected or not.
            try:
                command = command_queue.get_nowait()
                process_terminal_command(command, sock)
            except queue.Empty:
                pass

            if not running:
                break

            # Process cloud-triggered photo request in the main thread.
            if cloud_capture_event.is_set():
                cloud_capture_event.clear()
                photo_path = capture_photo(source="cloud-command")
                if photo_path:
                    try:
                        queue_photo_for_cloud(photo_path, "on_demand")
                    except OSError as error:
                        log(f"[OUTBOX ERROR] Could not queue cloud-requested photo: {error}")

            # Sample latest sensor value exactly once per minute.
            now_mono = time.monotonic()
            if now_mono >= next_sensor_sample:
                sample_and_queue_sensor()
                next_sensor_sample += SENSOR_INTERVAL_SECONDS
                if next_sensor_sample < now_mono:
                    next_sensor_sample = now_mono + SENSOR_INTERVAL_SECONDS

            # Retry queued cloud work when MQTT is connected.
            if now_mono >= next_outbox_flush:
                flush_outbox(max_items=5)
                next_outbox_flush = now_mono + 5

            # Receive ESP32 messages. The ESP32 can send faster than 1/min;
            # the Pi retains the newest value and only samples it once/min.
            if sock is not None:
                try:
                    received = sock.recv(1024)
                    if not received:
                        raise ConnectionError("ESP32 disconnected")

                    buffer += received.decode("utf-8", errors="replace")
                    while "\n" in buffer:
                        message, buffer = buffer.split("\n", 1)
                        if message.strip():
                            process_esp32_message(message.strip())

                except socket.timeout:
                    pass
                except (OSError, ConnectionError) as error:
                    log(f"[BT] Disconnected: {error}")
                    try:
                        sock.close()
                    except OSError:
                        pass
                    sock = None
                    next_bt_attempt = time.monotonic() + 2

            time.sleep(0.05)

    except KeyboardInterrupt:
        log("Stopping eFarm...")

    finally:
        running = False

        # Best-effort offline status; the MQTT Last Will also covers abrupt
        # power/network loss.
        if T_STATUS is not None and mqtt_connected.is_set():
            mqtt_publish(T_STATUS, {
                "schema_version": SCHEMA_VERSION,
                "device_id": DEVICE,
                "state": "offline",
                "ts": now_ms(),
                "fw": FW_VERSION,
            })

        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

        if mqtt_client is not None:
            try:
                mqtt_client.loop_stop()
                mqtt_client.disconnect()
            except Exception:
                pass

        log("[SYSTEM] Stopped. Local data and pending outbox are retained.")


if __name__ == "__main__":
    main()
