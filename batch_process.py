from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import Row
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame

sc = SparkContext.getOrCreate()
hiveCtx = HiveContext(sc)


def load_wordlist(filename):
    with open(filename,'r') as f:
        lines = f.read().splitlines()
    return lines


pwords = load_wordlist('positive.txt')
nwords = load_wordlist('negative.txt')



def extract_hashtag(ht):
	ht_clean = ht[5:-5].encode('utf-8').decode('ascii','ignore')
	hashtags = []
	for each in ht_clean.split(","):
		hashtags.append(each.split(":")[1][:-1])
	return hashtags

def check_sentiment(tweet):
    count = 0
    for word in tweet:
        if word in pwords:
            count += 1
        else:
            count -= 1
    if count >=0:
        return [1,0]
    else:
        return [0,1]


text = sc.hadoopFile('abc.txt', "org.apache.hadoop.mapred.TextInputFormat", "org.apache.hadoop.io.Text", "org.apache.hadoop.io.LongWritable")


textmap = text.map(lambda x: (extract_hashtag(x[1].split("\x03")[1]), (x[1].split("\x03")[3].split('{')[1][5:-2]).encode('utf-8').decode('ascii','ignore')))

tweetmap = textmap.map(lambda x: (x[0], check_sentiment(x[1])))

hashtagmap = tweetmap.flatMap(lambda x: ((str(y[1:-1].decode('ascii','ignore')),x[1]) for y in x[0] ))

r = hashtagmap.map(lambda x: x).reduceByKey(lambda m, n: [m[0] + n[0], m[1] + n[1]])

# Create dataframe and hive table
df = r.map(lambda x: Row(hashtag=x[0], pos=x[1][0], neg=x[1][1])).toDF(['hashtag', 'pos', 'neg'])
df.registerTempTable("temp_t")
aggRDD = hiveCtx.sql("select * from temp_t")
aggRDD.write.saveAsTable("tweet")


#textmap.saveAsTextFile('test/test1.csv')
