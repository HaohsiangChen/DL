from kafka import KafkaProducer
import cv2
import base64
import json
import time
import datetime as dt
import logging

class VideoCamera(object):
    def __init__(self):
        # Using OpenCV to capture from device 0. If you have trouble capturing
        # from a webcam, comment the line below out and use a video file
        # instead.
        self.video = cv2.VideoCapture(0)
       	self.interval = 1
       	self.source = 'v3.mp4'
       	self.camera_id = 'object_cam'
       	self.server = 'master:6667'
       	self.topic = 'test'
       	logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s',
       		level=logging.INFO)
       	self.logger = logging.getLogger(__name__)
        # If you decide to use video.mp4, you must have this file in the folder
        # as the main.py.
        # self.video = cv2.VideoCapture('video.mp4')
        # Connection to Kafka Enpoint
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.server,
                                          value_serializer=lambda m: json.dumps(m).encode('utf8'))
        except Exception as e:
            self.logger.error(e)

        # Start Streaming...
        self.logger.info('-'*50)

    def stream_video(self,image):
        """Start streaming video frames to Kafka forever."""
        
        timestamp = dt.datetime.now().isoformat()
        jpg_as_text = base64.b64encode(image).decode('utf-8')
        result = {
            'image': jpg_as_text,
            'timestamp': dt.datetime.now().isoformat(),
            'camera_id': self.camera_id
        }
        self.send_to_kafka(result)
        time.sleep(0.2)

    def send_to_kafka(self, data):

        self.producer.send(self.topic, data)
        self.logger.info('Sent image to Kafka endpoint.')


    def __del__(self):
        self.video.release()

    def get_frame(self):
        success, image = self.video.read()
        # We are using Motion JPEG, but OpenCV defaults to capture raw images,
        # so we must encode it into JPEG in order to correctly display the
        # video stream.
        ret, jpeg = cv2.imencode('.jpeg', image)
        return jpeg.tobytes(),jpeg