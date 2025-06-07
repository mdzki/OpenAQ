import time


def handle_rate_limit(headers: dict, attempt: int = 0):
    remaining = int(headers.get("x-ratelimit-remaining", 1))
    reset = headers.get("x-ratelimit-reset")
    now = int(time.time())

    if remaining == 0:
        if reset:
            reset_ts = int(reset)
            sleep_time = max(reset_ts - now, 1)
        else:
            sleep_time = min(2**attempt, 60)  # fallback: exponential backoff

        print(f"[RateLimit] Sleeping for {sleep_time}s (attempt {attempt})...")
        time.sleep(sleep_time)
