#!/usr/bin/env python
from __future__ import division, print_function, absolute_import

import os
from timeit import time
import warnings
import sys
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
import pyspark
from pyspark.streaming import kafka
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import tensorflow as tf

warnings.filterwarnings('ignore')
config = tf.ConfigProto()
config.gpu_options.allow_growth = True
sess = tf.Session(config=config)

def process_frame(dataset):
   # Definition of the parameters
    to_process = {}
    max_cosine_distance = 0.3
    nn_budget = None
    nms_max_overlap = 1.0
    producer = KafkaProducer(bootstrap_servers='master:6667',value_serializer=lambda m: json.dumps(m).encode('utf8'))
   # deep_sort 
    model_filename = 'model_data/mars-small128.pb'
    encoder = gdet.create_box_encoder(model_filename,batch_size=1)
    
    metric = nn_matching.NearestNeighborDistanceMetric("cosine", max_cosine_distance, nn_budget)
    tracker = Tracker(metric)
    data = dataset.collect()
    dt_now = dt.datetime.now()
    for datum in data:
        event = json.loads(datum[1])
        dt_event = dt.datetime.strptime(
            event['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
        delta = dt_now - dt_event
        print("timestamp = " + str(dt_event))
        if delta.seconds > 5:
            continue
        to_process[event['camera_id']] = event
    for key, event in to_process.items():
        t1 = time.time()
        event = json.loads(datum[1])
        try:
           decoded = base64.b64decode(event['image'])   
        except TypeError:
            return

        filename = '/home/haohsiang/Vigilancia-Distributed/codev1frame.jpg'  # I assume you have a way of picking unique filenames
        with open(filename, 'wb') as f:
           f.write(decoded)
        frame = cv2.imread(filename)
        #ret, frame = video_capture.read()  # frame shape 640*480*3
        image = Image.fromarray(frame[...,::-1]) #bgr to rgb
        boxs = yolo.detect_image(image)
        print("box_num",len(boxs))
        features = encoder(frame,boxs)

        # score to 1.0 here).
        detections = [Detection(bbox, 1.0, feature) for bbox, feature in zip(boxs, features)]

        # Run non-maxima suppression.
        boxes = np.array([d.tlwh for d in detections])
        scores = np.array([d.confidence for d in detections])
        indices = preprocessing.non_max_suppression(boxes, nms_max_overlap, scores)
        detections = [detections[i] for i in indices]

        # Call the tracker
        tracker.predict()
        tracker.update(detections)

        for track in tracker.tracks:
            if not track.is_confirmed() or track.time_since_update > 1:
                continue
            bbox = track.to_tlbr()
            cv2.rectangle(frame, (int(bbox[0]), int(bbox[1])), (int(bbox[2]), int(bbox[3])),(255,255,255), 2)
            cv2.putText(frame, str(track.track_id),(int(bbox[0]), int(bbox[1])),0, 5e-3 * 200, (0,255,0),2)
        for det in detections:
            bbox = det.to_tlbr()
            cv2.rectangle(frame,(int(bbox[0]), int(bbox[1])), (int(bbox[2]), int(bbox[3])),(255,0,0), 2)

        if len(boxs) > 0:
            print(str(track.track_id) + ' :' + str(bbox[0]) + ' ' + str(bbox[1]) + ' ' + str(bbox[2]) + ' ' + str(
                bbox[3]))
            print(dt.datetime.now().time())
            result = {
                'ID': str(track.track_id),
                'timestamp': dt.datetime.now().isoformat(),
                'location_x': str(bbox[0]),
                'w': str(bbox[2])
            }
            producer.send('resultstream', result)
            producer.flush()

        # if writeVideo_flag:
            ##save a frame
            # out.write(frame)
            # frame_index = frame_index + 1
            # list_file.write(str(frame_index)+' ')
            # if len(boxs) != 0:
                # for i in range(0,len(boxs)):
                    # list_file.write(str(boxs[i][0]) + ' '+str(boxs[i][1]) + ' '+str(boxs[i][2]) + ' '+str(boxs[i][3]) + ' ')
            # list_file.write('\n')
        # fps = 0.0
        # fps  = ( fps + (1./(time.time()-t1)) ) / 2
        # print("fps= %f"%(fps))
        
        # Press Q to stop!
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break


def main():
    #interval=10
    topic_to_consume='video'
    topic_for_produce='resultstream'
    kafka_endpoint='master:6667'
    producer = KafkaProducer(bootstrap_servers=kafka_endpoint)
    global yolo 
    yolo = YOLO()
    sc = SparkContext(appName='VideoTics')
    ssc = StreamingContext(sc, 3)  # , 3)

    # Make Spark logging less extensive
    log4jLogger = sc._jvm.org.apache.log4j
    log_level = log4jLogger.Level.ERROR
    log4jLogger.LogManager.getLogger('org').setLevel(log_level)
    log4jLogger.LogManager.getLogger('akka').setLevel(log_level)
    log4jLogger.LogManager.getLogger('kafka').setLevel(log_level)

    video = kafka.KafkaUtils.createDirectStream(
        ssc, [topic_to_consume],{'metadata.broker.list': kafka_endpoint})

    video.foreachRDD(process_frame)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()