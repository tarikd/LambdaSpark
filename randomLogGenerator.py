from multiprocessing import Process
from kafka import SimpleProducer, KafkaClient, KafkaConsumer, MultiProcessConsumer
import random, hashlib
from datetime import datetime
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt
import matplotlib.pyplot as plt

# To send messages synchronously
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

# Constants
NumPublishers = 5
Publishers = map(lambda x: "publisher_" + str(x), range(0, NumPublishers))

NumAdvertisers = 3
Advertisers = map(lambda x: "advertiser_" + str(x), range(0, NumAdvertisers))

UnknownGeo = "unknown"
Geos = ["NY", "CA", "FL", "MI", "HI", UnknownGeo]


def randomLog():
	i = 0
	while True:
		timestamp = str(datetime.now()).replace('-','').replace(' ', '').replace(':', '')
		publisher = random.choice(Publishers)
		advertiser = random.choice(Advertisers)
		website = "website_" + str(random.randint(0, 10000)) + ".com"
		cookie = "cookie_" + str(random.randint(0, 10000))
		geo = random.choice(Geos)
		bid = random.random()

		log = '{0}, {1}, {2}, {3}, {4}, {5}, {6}'.format(
			timestamp, str(publisher), str(advertiser), website, cookie, geo, str(bid))
		
		producer.send_messages("adnetwork_topic", log)

		i += 1
		if i % 1000 == 0:
			print ">>>>>>>>>> Sent %d messages <<<<<<<<<<" % i

def sparkStream():
	# Create a local StreamingContext with four working thread and batch
	# interval of 10 second
	sc = SparkContext("local[4]", "adnetwork_stream")
	ssc = StreamingContext(sc, 10) # 10 seconds batch

	zkQuorum = "localhost:2181"
	groupId = "spark-streaming-consumer"
	topics = {"adnetwork_topic": 1}

	messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics, storageLevel = StorageLevel.MEMORY_AND_DISK_SER)
	
	# # Data manipulation & count
	# lines = messages.map(lambda x: x[1])
	# counts = lines.flatMap(lambda line: line.split(", ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
	# counts.saveAsTextFiles("count", "txt")
	# counts.pprint()
	
	# Machine Learning
	messages.foreachRDD(kMeansRDD)

	ssc.start()
	ssc.awaitTermination()

def parseVector(lines):
	return array([float(int(hashlib.md5(x).hexdigest(), base=16)) for x in str(lines[1]).split(', ')])
    #return array([float(x) for x in line.split(', ')])


def kMeansRDD(rdd):
	# Load and parse the data
	parsedData = rdd.map(parseVector).cache()

	# Build the model (cluster the data)
	clusters = KMeans.train(parsedData, 4, maxIterations=10,
	        runs=10, initializationMode="random")

	producer.send_messages("clusters_centers", str(clusters.centers))

	# Evaluate clustering by computing Within Set Sum of Squared Errors
	def error(point):
	    center = clusters.centers[clusters.predict(point)]
	    return sqrt(sum([x**2 for x in (point - center)]))

	WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
	print("Within Set Sum of Squared Error = " + str(WSSSE))


if __name__ == '__main__':
	process_randomLog = Process(target=randomLog)
	process_sparkStream = Process(target=sparkStream)
	process_randomLog.start()
	process_sparkStream.start()
	process_sparkStream.join()
	process_randomLog.join()


