import datetime, os

def iso_utc():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str):
    print(f"[{iso_utc()}] {msg}", flush=True)

def tz_name():
    return os.getenv("TZ", "Europe/Istanbul")
