from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_ids = ['G1', 'G2', 'G3']

while True:
    for gudang in gudang_ids:
        suhu = random.randint(30, 80)
        data = {"gudang_id": gudang, "kelembapan": suhu}
        producer.send("sensor-kelembapan-gudang", value=data)
        print(f"Sent: {data}")
        time.sleep(3)
    time.sleep(5)