import time


def handle_rate_limit(headers: dict):
    remaining = int(headers.get("x-ratelimit-remaining", 1))
    reset = int(headers.get("x-ratelimit-reset", 0))
    now = int(time.time())

    if remaining == 0:
        sleep_time = max(reset - now, 0)
        if sleep_time > 600:
            raise RuntimeError("Too long wait due to rate limit")
        print(f"Sleeping {sleep_time} seconds due to rate limiting...")
        time.sleep(sleep_time)
