import base64
import cv2
import json
import time
import numpy as np
import kafka as kafka_client
import datetime as dt

def _main():
    video_capture = cv2.VideoCapture(0)
    topic = 'test'
    while True:
        ret, frame = video_capture.read()  # frame shape 640*480*3
        if ret != True:
           break
        t1 = time.time()
        s, buffer = cv2.imencode('.jpg', frame)
        jpg_as_text = base64.b64encode(buffer).decode('utf-8')

        payload = {
            'image': jpg_as_text,
            'timestamp': dt.datetime.now().isoformat()
        }

        producer = kafka_client.KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda m: json.dumps(m).encode('utf8'))

        producer.send(topic, payload)
        fps = 0.0    
        fps  = ( fps + (1./(time.time()-t1)) ) / 2
        print("fps= %f"%(fps))
def main():
    _main()


if __name__ == '__main__':
    main()