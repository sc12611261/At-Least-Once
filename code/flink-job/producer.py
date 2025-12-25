from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# 配置信息
KAFKA_BOOTSTRAP_SERVERS = '49.52.27.49:9092' # 确保IP与你的集群一致
TARGET_TOPIC = 'flink-test-topic'
MSG_RATE = 100         # 每秒发送100条
MAX_MESSAGES = 30000    # 生产3000条后自动停止
MSG_ID_START = 1

def on_send_success(record_metadata):
    # 为了防止控制台刷新过快影响性能，你可以选择只打印部分日志
    pass 

def on_send_error(excp):
    print(f"发送错误: {excp}")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        batch_size=16384,
        linger_ms=10,
    )

if __name__ == "__main__":
    producer = create_producer()
    cur_msg_id = MSG_ID_START
    count = 0 # 计数器
    
    print(f"[{datetime.now()}] 开始生产数据，目标数量: {MAX_MESSAGES}...")

    try:
        while count < MAX_MESSAGES:
            data = {
                "temperature": round(random.uniform(20.0, 35.0), 2),
                "humidity": round(random.uniform(30.0, 80.0), 1),
                "timestamp": int(time.time()),
            }
            msg = {
                "msg_id": cur_msg_id,
                "content": data,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            }

            # 发送消息
            producer.send(TARGET_TOPIC, msg).add_callback(on_send_success).add_errback(on_send_error)
            
            cur_msg_id += 1
            count += 1
            
            # 控制发送频率
            time.sleep(1 / MSG_RATE)
            
            # 每发送500条在控制台提示一次进度
            if count % 500 == 0:
                print(f"已发送: {count} / {MAX_MESSAGES}")

        print(f"\n[{datetime.now()}] 已达到目标数量 {MAX_MESSAGES}，正在关闭...")
        
    except KeyboardInterrupt:
        print(f"\n用户手动停止。")
    finally:
        producer.flush() # 确保所有内存中的消息都发往Kafka
        producer.close()
        print(f"生产任务结束。起始ID: {MSG_ID_START}, 最终ID: {cur_msg_id - 1}")