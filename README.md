 # person traker using kafka and spark-streaming
 
 ## How to use
	Download and Install Kafka and Spark
	
	https://kafka.apache.org/quickstart
	https://spark.apache.org/downloads.html
	
	The following dependencies are needed to run the tracker:

	NumPy
	sklearn
	OpenCV
	Pillow
	TensorFlow
	Kafka
	Pyspark
	
	Download https://drive.google.com/file/d/1uvXFacPnrSMw6ldWTyLLjGLETlEsUvcE/view?usp=sharing (yolo.h5 model file with tf-1.4.0) , put it into model_data folder

    Run traker with cmd :

	python producer.py
	python spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.3.jar tracker.py


	

