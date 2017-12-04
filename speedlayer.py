
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import re


def main():
    conf = SparkConf().setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    
    counts = stream(ssc, pwords, nwords, 100)

def load_wordlist(filename):
    f= open(filename,'r')
    lines=f.read().splitlines()
    return lines

def check_line(line, pwords,nwords):
    count=0
    for word in line:
        if word in pwords:
            count+=1
        elif word in nwords:
            count-=1
    if count>=0:
        return [1,0]
    else:
        return [0,1] 


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['topic_ls3'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    #tweets.pprint()
    #lines = tweets.map(lambda line: line)
    tweet_map = tweets.map(lambda line:(re.findall(r"#(\w+)",line),(check_line(line,pwords,nwords))))
    tag_map = tweet_map.flatMap(lambda x:((y,x[1]) for y in x[0]))
    result = tag_map.map(lambda x: x).reduceByKey(lambda m,n:[m[0] + n[0], m[1] + n[1]])
    result.pprint()

    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return result


if __name__=="__main__":
    main()
