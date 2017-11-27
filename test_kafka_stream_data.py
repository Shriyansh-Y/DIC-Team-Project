from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import threading
import time
import multiprocessing
from kafka import KafkaConsumer, KafkaProducer

import boto3

access_token = "601165399-EhXKtLIiYaYxjQxLMCa1iGWd5UZ2QH0WOf9glWVq"
access_token_secret = "kCUDnfK2BvN6IavgTjqCnSB9LaZ1necAe7OOVNfFLRSlg"
consumer_key = "glaUDutQB0Q4Z9ir4LF3tHQ1Y"
consumer_secret = "6QDpPmnHVLIEGaPuVLcYIjli8xg8Zy2aqjvCnKnJDXuhtQ0GZh"

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('Tweets')


class StdOutListener(StreamListener):

    def on_status(self, status):
        try:
            text = status.extended_tweet["full_text"]
        except AttributeError:
            text = status.text
        i = str(status.id)
        producer.send('blackfriday', key=i.encode("utf-8"), value=text.encode("utf-8"))
        return True

    def on_error(self, status):
        print(status)


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['blackfriday'])

        while not self.stop_event.is_set():
            # with table.batch_writer(overwrite_by_pkeys=['id', 'keyword']) as batch:
            for message in consumer:
                print(message.topic, message.key.decode("utf-8"), message.value.decode("utf-8"))
                # batch.put_item(
                #     Item={
                #         'id': message.key.decode("utf-8"),
                #         'keyword': message.topic,
                #         'text': message.value.decode("utf-8")
                #     }
                # )
                if self.stop_event.is_set():
                    break

        consumer.close()


def stream():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    l = StdOutListener()
    stream = Stream(auth, l)
    stream.filter(track=["blackfriday"])


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
p = threading.Thread(target=stream)
c = Consumer()
p.start()
c.start()

time.sleep(15)

producer.close()
p.stop()
c.stop()
p.join()
c.join()
