from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "601165399-EhXKtLIiYaYxjQxLMCa1iGWd5UZ2QH0WOf9glWVq"
access_token_secret =  "kCUDnfK2BvN6IavgTjqCnSB9LaZ1necAe7OOVNfFLRSlg"
consumer_key =  "glaUDutQB0Q4Z9ir4LF3tHQ1Y"
consumer_secret =  "6QDpPmnHVLIEGaPuVLcYIjli8xg8Zy2aqjvCnKnJDXuhtQ0GZh"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")
