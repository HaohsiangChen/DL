"""Person Track on Spark using YOLO.

Consumes video frames from an Kafka Endpoint, process it on spark, produces
a result containing annotate video frame and sends it to another topic of the
same Kafka Endpoint.

"""

#!/usr/bin/env python
from __future__ import division, print_function, absolute_import

import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
from timeit import default_timer as timer
import warnings
import sys
from random import randint
import cv2
import numpy as np
from PIL import Image
from yolo import YOLO
import datetime as dt
import json
from deep_sort import preprocessing
from deep_sort import nn_matching
from deep_sort.detection import Detection
from deep_sort.tracker import Tracker
from tools import generate_detections as gdet
from deep_sort.detection import Detection as ddet
from kafka import KafkaProducer
import base64
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import tensorflow as tf

# config = tf.ConfigProto()
# config.gpu_options.allow_growth = True
# sess = tf.Session(config=config)
gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.5)
config = tf.ConfigProto(gpu_options=gpu_options)
session = tf.Session(config=config)
tf.logging.set_verbosity(tf.logging.ERROR)

class Spark_Tracker():
    """Stream WebCam Images to Kafka Endpoint."""

    def __init__(self,
                 interval=0.1,
                 topic_to_consume='test',
                 topic_for_produce='position',
                 kafka_endpoint='master:6667'):

        """Initialize our yolo model."""
        self.yolo = YOLO()

        # Create Kafka Producer for sending results
        self.topic_to_consume = topic_to_consume
        self.topic_for_produce = topic_for_produce
        self.kafka_endpoint = kafka_endpoint
        self.producer = KafkaProducer(bootstrap_servers=kafka_endpoint, value_serializer=lambda m: json.dumps(m).encode('utf8'))

        """Initialize Spark environment."""

        sc = SparkContext(appName='VideoTics')
        self.ssc = StreamingContext(sc, interval)  # , 3)

        # Make Spark logging less extensive
        log4jLogger = sc._jvm.org.apache.log4j
        log_level = log4jLogger.Level.ERROR
        log4jLogger.LogManager.getLogger('org').setLevel(log_level)
        log4jLogger.LogManager.getLogger('akka').setLevel(log_level)
        log4jLogger.LogManager.getLogger('kafka').setLevel(log_level)
        self.logger = log4jLogger.LogManager.getLogger(__name__)
        self.objects_detected_view_text=""

        # Set deep_sort param
        self.max_cosine_distance = 0.3
        self.nn_budget = None
        self.model_filename = 'model_data/mars-small128.pb'
        self.nms_max_overlap = 1.0
        self.encoder = gdet.create_box_encoder(self.model_filename, batch_size=1)
        self.metric = nn_matching.NearestNeighborDistanceMetric("cosine", self.max_cosine_distance, self.nn_budget)
        self.tracker = Tracker(self.metric)

    def start_processing(self):
        """Start consuming from Kafka endpoint and detect objects."""
        kvs = KafkaUtils.createDirectStream(self.ssc,
                                            [self.topic_to_consume],
                                            {'metadata.broker.list': self.kafka_endpoint}
                                            )
        kvs.foreachRDD(self.process_frame)
        self.ssc.start()
        self.ssc.awaitTermination()

    def detect_person_track(self, event):

        """Use Yolo to detect person."""
        try:
            decoded = base64.b64decode(event['image'])
        except TypeError:
            return

        # TODO: Picking unique filenames or find a way to send it to kafka

        filename = 'codev1frame.jpg'  # find a way to pick unique filenames
        with open(filename, 'wb') as f:
            f.write(decoded)
        frame = cv2.imread(filename)
        image = Image.fromarray(frame[..., ::-1])  # bgr to rgb
        boxs = self.yolo.detect_image(image)
        #print("box_num", len(boxs))
        features = self.encoder(frame, boxs)

        # score to 1.0 here).
        detections = [Detection(bbox, 1.0, feature) for bbox, feature in zip(boxs, features)]

        # Run non-maxima suppression.
        boxes = np.array([d.tlwh for d in detections])
        scores = np.array([d.confidence for d in detections])
        indices = preprocessing.non_max_suppression(boxes, self.nms_max_overlap, scores)
        detections = [detections[i] for i in indices]

        """Use deep-sort to track person."""
        # Call the tracker
        self.tracker.predict()
        self.tracker.update(detections)

        for track in self.tracker.tracks:
            if not track.is_confirmed() or track.time_since_update > 1:
                 continue
            bbox = track.to_tlbr()
            cv2.rectangle(frame, (int(bbox[0]), int(bbox[1])), (int(bbox[2]), int(bbox[3])), (255, 255, 255), 2)
            cv2.putText(frame, str(track.track_id), (int(bbox[0]), int(bbox[1])), 0, 5e-3 * 200, (0, 255, 0), 2)
        for det in detections:
            bbox = det.to_tlbr()
            cv2.rectangle(frame, (int(bbox[0]), int(bbox[1])), (int(bbox[2]), int(bbox[3])), (255, 0, 0), 2)
        # sent result to kafka
        if len(boxs) > 0:
            for i in range(0, len(boxs)):
                self.objects_detected_view_text = 'ID:' + str(track.track_id) + '  x:' + str(boxs[i][0]) + '  y:' + str(boxs[i][1]) + '  width:' + str(
                    boxs[i][2]) + '  height:' + str(boxs[i][3])
                result = {
                    'ID': str(self.tracker.tracks[i].track_id),
                    'timestamp': dt.datetime.now().isoformat(),
                    'location_x': str(boxs[i][0]),
                    'w': str(boxs[i][2]),
                    'image': self.convert_image_to_text(frame)
                }
                self.producer.send('position', result)
                self.producer.flush()
                self.logger.info('prediction: ' + self.objects_detected_view_text)
            return

    def convert_image_to_text(self,frame):
        img_str = cv2.imencode('.jpeg', frame)[1]
        img_as_text = base64.b64encode(img_str).decode('utf-8')
        return img_as_text


    def process_frame(self, timestamp, dataset):
        # Definition of the parameters
        to_process = {}
        data = dataset.collect()
        self.logger.info( '\033[3' + str(randint(1, 7)) + ';1m' +  # Color
            '-' * 25 +
            '[ NEW MESSAGES: ' + str(len(data)) + ' ]'
            + '-' * 25 +
            '\033[0m' # End color
            )
        dt_now = dt.datetime.now()

        for datum in data:
            event = json.loads(datum[1])
            self.logger.info('Received Message: ' +
                             event['camera_id'] + ' - ' + event['timestamp'])
            dt_event = dt.datetime.strptime(
                event['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            delta = dt_now - dt_event
            #print("timestamp = " + str(dt_event))
            if delta.seconds > 5:
                continue
            to_process[event['camera_id']] = event

        if len(to_process) == 0:
            self.logger.info('Skipping processing...')

        for key, event in to_process.items():
            self.logger.info('Processing Message: ' +
                             event['camera_id'] + ' - ' + event['timestamp'])
            start = timer()
            detection_result = self.detect_person_track(event)

            end = timer()
            delta = end - start
            self.logger.info('Done after ' + str(delta) + ' seconds.')

            try:
                if detection_result:
                    self.logger.info('Sent image to Kafka endpoint.')

            except AssertionError:
                self.objects_detected_view_text = 'No person found!'
                continue
            # Press Q to stop!
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

if __name__ == '__main__':
    sod = Spark_Tracker(
        interval=0.1,
        topic_to_consume='test',
        topic_for_produce='position',
        kafka_endpoint='master:6667')
    sod.start_processing()