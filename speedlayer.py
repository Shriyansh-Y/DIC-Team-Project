from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np


def main():
    conf = SparkConf().setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    
    counts = stream(ssc, pwords, nwords, 100)


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    f= open(filename,'r')
    lines=f.read().splitlines()
    return lines
        
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


def check_word(word, pwords,nwords):
    if word in pwords:
      return ('positive')
    elif word in nwords:
      return ('negative')
    else:
      return ('nothing')

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['topic_ls3'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE


    #tweets.pprint()
    words = tweets.flatMap(lambda line: line.split(" "))
    word_tag = words.map(lambda x:(check_word(x,pwords,nwords),1))
    filtered_word_tag=word_tag.filter(lambda x: x[0]=='positive' or x[0]=='negative')
    wordCounts = filtered_word_tag.reduceByKey(lambda x, y: x + y)
    runningCounts = wordCounts.updateStateByKey(updateFunction)
    # Let the counts variable hold the word counts for all time steps
    
    
    wordCounts.pprint()
    #dstream.foreachRDD(lambda rdd: rdd.foreach(sendRecord))

    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    # Start the computation
    ssc.start()                        
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()

