from multiprocessing import Process
from kafka import SimpleProducer, KafkaClient, KafkaConsumer, MultiProcessConsumer, SimpleConsumer
from numpy import array
from math import sqrt
import matplotlib.pyplot as plt

# To send messages synchronously
kafka = KafkaClient("localhost:9092")

def multiProcessKafkaConsumer():

	consumer = MultiProcessConsumer(kafka, group="spark-streaming-consumer", topic="clusters_centers", num_procs=2)

	allMessages = []
	for message in consumer.__iter__():
	 	allMessages.append(message.message.value)

	consumer.stop()
	return allMessages

def cleanKafkaMessage(kafkaMessage, first, last):
	kafkaMessage = kafkaMessage.replace("array([  ", "").replace("])", "").replace("\n", " ")
	kafkaMessage = ' '.join(kafkaMessage.split()).replace("[", " ").strip("]")

	i = 0	
	listClustersCentersString = []
	while i < len(kafkaMessage)+14:
		start = kafkaMessage.index(first) + len(first)
		try:
			end = kafkaMessage.index(last, start)
		except ValueError:
			print ""
		clusterCenter = kafkaMessage[start:end]
		listClustersCentersString.append(float(clusterCenter))
		kafkaMessage = kafkaMessage[end:]
		i+=1
	return array(listClustersCentersString)


def clean():
	kafkaMessages = multiProcessKafkaConsumer()
	for message in kafkaMessages:
		clean = cleanKafkaMessage(message, " ", ",")
		print clean

if __name__ == '__main__':
	clean()












