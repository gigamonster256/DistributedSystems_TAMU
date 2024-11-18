import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SleepBucket {
  public static class TokenizerMapper extends Mapper<Object, Tweet, IntWritable, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private IntWritable hour = new IntWritable();

    public void map(Object key, Tweet value, Context context)
        throws IOException, InterruptedException {
      // filter to only include tweets that contain the word "sleep"
      if (!value.getContent().toLowerCase().contains("sleep")) {
        return;
      }
      hour.set(value.getDateTime().getHour());
      context.write(hour, one);
    }
  }

  public static class IntSumReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sleep bucket");
    job.setJarByClass(SleepBucket.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setInputFormatClass(TweetInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    TweetInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("output_sleep"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
