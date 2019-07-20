import datetime
from flask import Flask, Response
from kafka import KafkaConsumer
import json
import base64
import cv2
import numpy as np
from flask import Flask, render_template, Response

# Fire up the Kafka Consumer
topic = "position"

consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=['master:6667'])


# Set the consumer in a Flask App
app = Flask(__name__)


@app.route('/')
def index():
    return render_template('video.html')

@app.route('/video', methods=['GET'])
def video():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """

    img_array = []
    for msg in consumer:
        json_from_consumer = json.loads(msg[-6].decode('utf-8'))
        print(json_from_consumer['image'])
        decoded = base64.b64decode(json_from_consumer['image'])
        filename = 'resultframe.jpg'  # I assume you have a way of picking unique filenames
        with open(filename, 'wb') as f:
            f.write(decoded)
        img = cv2.imread(filename)
        height, width, layers = img.shape
        size = (width, height)
        img_array.append(img)

        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + decoded + b'\r\n\r\n')
    out = cv2.VideoWriter('project.avi', cv2.VideoWriter_fourcc(*'DIVX'), 15, size)

    for i in range(len(img_array)):
        out.write(img_array[i])
    out.release()

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=5002,threaded=True, debug=True)