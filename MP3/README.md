## MapReduce
Build jar file
```bash
hadoop com.sun.tools.javac.Main *.java && jar cf tweet.jar *.class
```

Run MapReduce
```bash
hadoop jar tweet.jar $MapReduceFunction $inputFolder
```

Available MapReduce functions:
- `TweetCounter`: Count the number of tweets in the input folder
- `TimeBucket`: Count the number of tweets in each hour
- `SleepBucket`: Count the number of tweets in each hour that reference the word "sleep"

The MapReduceFunctions expect a folder as input containing files with the following format:
```
total number: #ofTweets
T   yyyy-MM-dd HH:mm:ss
U   http://twitter.com/username
W   tweet text
(Empty line)
T   yyyy-MM-dd HH:mm:ss
U   http://twitter.com/username
and so on...
```