import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TweetRecordReader extends RecordReader<LongWritable, Tweet> {
  private LineRecordReader lineReader = new LineRecordReader();
  private LongWritable key = new LongWritable();
  private Tweet tweet;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    lineReader.initialize(split, context);
  }

  // expected format:
  // T\t2009-06-01 21:43:59
  // U\thttp://twitter.com/burtonator
  // W\tNo Post Title
  // Contents of the post
  @Override
  public boolean nextKeyValue() throws IOException {
    long currentTimestamp = 0;
    Text currentUser = new Text();
    Text currentTitle = new Text();
    Text currentContent = new Text();

    while (lineReader.nextKeyValue()) {
      String line = lineReader.getCurrentValue().toString().trim();

      // ignore total number line at beginning of file
      if (line.startsWith("total number:")) {
        continue;
      }
      if (line.startsWith("T\t")) {
        // strip the "T\t" prefix
        Text timestamp = new Text(line.substring(2).trim());
        final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_FORMAT);
        try {
          currentTimestamp = sdf.parse(timestamp.toString()).getTime();
        } catch (ParseException e) {
          System.err.println("Error parsing timestamp: " + timestamp);
          e.printStackTrace();
        }
      } else if (line.startsWith("U\t")) {
        final String USER_PREFIX = "U\thttp://twitter.com";
        String user = line.substring(USER_PREFIX.length()).trim();
        // trim leading / if it exists
        if (user.startsWith("/")) {
          user = user.substring(1);
        }
        currentUser.set(user);
      } else if (line.startsWith("W\t")) {
        currentTitle = new Text(line.substring(2).trim());
      } else {
        // consume until next record (line beginning with "T ")
        currentContent.append(line.getBytes(), 0, line.length());
        currentContent.append("\n".getBytes(), 0, 1);
      }

      // Check if we've completed a tweet record
      if (line.startsWith("T\t") && currentContent.getLength() > 0) {
        key.set(currentTimestamp);
        tweet = new Tweet(new LongWritable(currentTimestamp), currentUser, currentTitle,
            new Text(currentContent.toString().trim()));
        return true;
      }
    }

    // Last tweet in the file
    if (currentContent.getLength() > 0) {
      key.set(currentTimestamp);
      tweet = new Tweet(new LongWritable(currentTimestamp), currentUser, currentTitle,
          new Text(currentContent.toString().trim()));
      return true;
    }

    return false;
  }

  @Override
  public LongWritable getCurrentKey() {
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