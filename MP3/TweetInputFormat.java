import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TweetInputFormat extends FileInputFormat<IntWritable, Tweet> {
  @Override
  public RecordReader<IntWritable, Tweet> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new TweetRecordReader();
  }

  // don't split the file (may cut tweets in 2)
  // ideally we would store each tweet on one line to avoid this and allow splitting
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
}