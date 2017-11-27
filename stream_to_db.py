from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import umsgpack

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
        tags = [t['text'] for t in status.entities['hashtags']]
        if tags:
            v = umsgpack.packb([tags, text])
            producer.send('topic_ls0', key=i.encode('utf-8'), value=v)
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
        consumer.subscribe(['topic_ls0'])

        while not self.stop_event.is_set():
            with table.batch_writer(overwrite_by_pkeys=['id']) as batch:
                for message in consumer:
                    # print(message.topic, message.key.decode("utf-8"), umsgpack.unpackb(message.value))
                    tags, text = umsgpack.unpackb(message.value)
                    batch.put_item(
                        Item={
                            'id': message.key.decode("utf-8"),
                            'hashtags': tags,
                            'text': text
                        }
                    )
                    if self.stop_event.is_set():
                        break

        consumer.close()


def stream():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    l = StdOutListener()
    stream = Stream(auth, l)
    stream.filter(track=["#"])


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
p = threading.Thread(target=stream)
c = Consumer()
p.start()
c.start()

time.sleep(600)

producer.close()
c.stop()
p.join()
c.join()
