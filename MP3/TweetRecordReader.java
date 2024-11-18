import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

// takes in a tweet file and returns index, tweet pairs
public class TweetRecordReader extends RecordReader<IntWritable, Tweet> {
  private final static String timestamp_prefix = "T\t";
  private final static String[] user_prefixes = {
      "U\thttp://twitter.com/", "U\thttp://twitter.com", "U\thttp://www.twitter.com/"};
  private final static String content_prefix = "W\t";

  private LineRecordReader lineReader = new LineRecordReader();
  private IntWritable key = new IntWritable(0);
  private Tweet tweet;

  enum state {
    BEGIN,
    TIMESTAMP,
    USERNAME,
    CONTENT,
    FINISHED,
  }

  private state currentState = state.BEGIN;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    assert currentState == state.BEGIN : "Expected state to be BEGIN, got: " + currentState;
    lineReader.initialize(split, context);
    // consume total number line
    assert lineReader.nextKeyValue() : "Expected total number line";
    currentState = state.TIMESTAMP;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (currentState == state.FINISHED) {
      return false;
    }

    String timestamp = null;
    String username = null;
    String content = null;

    while (lineReader.nextKeyValue()) {
      String line = lineReader.getCurrentValue().toString().trim();

      // skip empty lines
      if (line.isEmpty()) {
        continue;
      }

      switch (currentState) {
        case TIMESTAMP:
          assert line.startsWith(timestamp_prefix) : "Expected timestamp prefix, got: " + line;
          timestamp = line.substring(timestamp_prefix.length());
          currentState = state.USERNAME;
          break;
        case USERNAME:
          boolean found = false;
          for (String prefix : user_prefixes) {
            if (line.startsWith(prefix)) {
              username = line.substring(prefix.length());
              found = true;
              break;
            }
          }
          assert found : "Expected username prefix, got: " + line;
          currentState = state.CONTENT;
          break;
        case CONTENT:
          assert line.startsWith(content_prefix) : "Expected content prefix, got: " + line;
          line = line.substring(content_prefix.length());
          key.set(key.get() + 1);
          tweet = new Tweet(timestamp, username, line);
          currentState = state.TIMESTAMP;
          return true;
      }
    }
    assert currentState
        == state.TIMESTAMP : "Expected state to be TIMESTAMP at end of file, got: " + currentState;
    currentState = state.FINISHED;
    return false;
  }

  @Override
  public IntWritable getCurrentKey() {
    return key;
  }

  @Override
  public Tweet getCurrentValue() {
    return tweet;
  }

  @Override
  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }
}