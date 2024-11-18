import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TweetInputFormat extends FileInputFormat<LongWritable, Tweet> {
  @Override
  public RecordReader<LongWritable, Tweet> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new TweetRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    return super.getSplits(job);
  }
}