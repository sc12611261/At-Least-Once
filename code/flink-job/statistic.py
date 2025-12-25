# from kafka import KafkaConsumer
# import json
# import sys
# from datetime import datetime

# KAFKA_BOOTSTRAP_SERVERS = 'gpu2.solelab.tech:9092'
# TARGET_TOPIC = 'flink-result-topic'
# msg_id_end = 0
# msg_id_start = 1

# def create_consumer():
#     consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                              value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#                              auto_offset_reset='earliest',
#                              group_id=f"stat-group-{datetime.now().timestamp()}",
#                              enable_auto_commit=False,
#                              consumer_timeout_ms=1000)
#     return consumer

# if __name__ == '__main__':
#     consumer = create_consumer()
#      received_msg_ids = []  
#      unique_msg_ids = set()
#      print(f"[{datetime.now()}] 开始统计，目标Topic：{TARGET_RESULT_TOPIC}，理论消息总数：{TOTAL_EXPECTED_MSG}")
     
#      try:
#          # 消费5分钟（足够接收所有消息，含故障恢复后的延迟消息）
#          end_time = datetime.now().timestamp() + 300
#          while datetime.now().timestamp() < end_time:
#              records = consumer.poll(timeout_ms=1000)  # 每秒轮询1次
#              for topic_partition, messages in records.items():
#                  for msg in messages:
#                      msg_content = msg.value
#                      original_msg_id = msg_content.get("original_msg_id")  # 需与作业中写入的字段名一致
#                      if original_msg_id and MSG_ID_START <= original_msg_id <= MSG_ID_END:
#                          received_msg_ids.append(original_msg_id)
#                          unique_msg_ids.add(original_msg_id)
#          # 计算核心指标
#          total_received = len(received_msg_ids)
#          total_unique_received = len(unique_msg_ids)
#          loss_rate = (TOTAL_EXPECTED_MSG - total_unique_received) / TOTAL_EXPECTED_MSG * 100
#          duplicate_rate = (total_received - total_unique_received) / total_received * 100 if total_received > 0 else 0
         
#          # 打印统计结果
#          print(f"\n=== [{datetime.now()}] 统计结果 ===")
#          print(f"1. 理论消息总数（唯一）：{TOTAL_EXPECTED_MSG}")
#          print(f"2. 实际接收消息总数（含重复）：{total_received}")
#          print(f"3. 实际接收唯一消息数：{total_unique_received}")
#          print(f"4. 消息丢失率：{loss_rate:.2f}%")
#          print(f"5. 消息重复率：{duplicate_rate:.2f}%")
#      except KeyboardInterrupt:
#          consumer.close()
#          sys.exit(1)
#      finally:
#         consumer.close()
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException,KafkaError
from collections import defaultdict

KAFKA_BOOTSTRAP_SERVERS = "49.52.27.49:9092"
KAFKA_TOPIC = "flink-result-topic"
GROUP_ID = "statistic-consumer-group"

TOTAL_EXPECTED_MSG = 30000  # 请根据实际情况修改！

# 消费者配置
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  
}

received_ids = set()
total_received = 0

def print_statistics():
    global total_received, received_ids
    total_unique_received = len(received_ids)
    
    if TOTAL_EXPECTED_MSG > 0:
        loss_rate = (TOTAL_EXPECTED_MSG - total_unique_received) / TOTAL_EXPECTED_MSG * 100
    else:
        loss_rate = 0.0

    if total_received > 0:
        duplicate_rate = (total_received - total_unique_received) / total_received * 100
    else:
        duplicate_rate = 0.0

    print(f"\n=== [{datetime.now()}] 统计结果 ===")
    print(f"1. 理论消息总数（唯一）：{TOTAL_EXPECTED_MSG}")
    print(f"2. 实际接收消息总数（含重复）：{total_received}")
    print(f"3. 实际接收唯一消息数：{total_unique_received}")
    print(f"4. 消息丢失率：{loss_rate:.2f}%")
    print(f"5. 消息重复率：{duplicate_rate:.2f}%")

def main():
    global total_received, received_ids

    consumer = Consumer(conf)
    try:
        consumer.subscribe([KAFKA_TOPIC])
        print(f"[{datetime.now()}] 开始消费 Kafka 主题: {KAFKA_TOPIC}")

        last_print = time.time()
        PRINT_INTERVAL = 10  # 每10秒打印一次统计

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            try:
                value = msg.value().decode('utf-8')
                data = json.loads(value)
                msg_id = data.get("original_msg_id")
                if msg_id is not None:
                    total_received += 1
                    received_ids.add(msg_id)
                else:
                    print(f"[WARN] 缺少 original_msg_id 字段: {value}")
            except Exception as e:
                print(f"[ERROR] 解析消息失败: {e}, 原始消息: {msg.value()}")

            # 定期打印统计
            now = time.time()
            if now - last_print >= PRINT_INTERVAL:
                print_statistics()
                last_print = now

    except KeyboardInterrupt:
        print("\n[INFO] 收到中断信号，正在关闭消费者...")
    finally:
        print_statistics()  
        consumer.close()

if __name__ == "__main__":
    main()
