from kafka import KafkaProducer
import cv2
import base64
import json
import time
import datetime as dt
import logging
from camera import VideoCamera
from flask import Flask, render_template, Response


app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def gen(camera):
    while True:
        frame,raw_frame = camera.get_frame()
        camera.stream_video(raw_frame)
        yield (b'--frame\r\n'
               b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n\r\n')

@app.route('/video_feed')
def video_feed():
    return Response(gen(VideoCamera()),
                    mimetype='multipart/x-mixed-replace; boundary=frame')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
