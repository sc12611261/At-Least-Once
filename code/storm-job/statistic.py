import json
import time
from kafka import KafkaConsumer
from datetime import datetime
from collections import defaultdict, deque

BOOTSTRAP_SERVERS = "49.52.27.49:9092"
TOPIC = "storm-result-topic"
GROUP_ID = "storm-metrics-realtime"

PRINT_INTERVAL = 15        # 每 5 秒输出一次
LATENCY_WINDOW = 200     # 最近 N 条计算延迟

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ===== 统计变量 =====
total_msgs = 0
received_counts = defaultdict(int)
latency_samples = deque(maxlen=LATENCY_WINDOW)

start_time = time.time()
last_print_time = start_time
last_total = 0

print("\n[INFO] Start real-time monitoring Storm topology...\n")

for msg in consumer:
    value = msg.value
    total_msgs += 1

    msg_id = value.get("original_msg_id")
    received_counts[msg_id] += 1

    # ===== 延迟计算 =====
    try:
        t0 = datetime.fromisoformat(value["origin_timestamp"])
        t1 = datetime.fromtimestamp(int(value["window_time"]) / 1000)
        latency_ms = (t1 - t0).total_seconds() * 1000
        latency_samples.append(latency_ms)
    except Exception:
        pass

    now = time.time()
    if now - last_print_time >= PRINT_INTERVAL:
        interval = now - last_print_time
        throughput = (total_msgs - last_total) / interval

        duplicates = sum(1 for v in received_counts.values() if v > 1)
        avg_latency = (
            sum(latency_samples) / len(latency_samples)
            if latency_samples else 0
        )
        p95_latency = (
            sorted(latency_samples)[int(len(latency_samples) * 0.95)]
            if len(latency_samples) >= 20 else 0
        )

        print("=" * 60)
        print(f"[TIME] {datetime.now().strftime('%H:%M:%S')}")
        print(f"Total outputs        : {total_msgs}")
        print(f"Unique msg_id        : {len(received_counts)}")
        print(f"Throughput           : {throughput:.2f} msg/s")
        print(f"Duplicate msg_id     : {duplicates}")
        print(f"Avg latency (recent) : {avg_latency:.2f} ms")
        print(f"P95 latency (recent) : {p95_latency:.2f} ms")
        print("=" * 60)

        last_print_time = now
        last_total = total_msgs
