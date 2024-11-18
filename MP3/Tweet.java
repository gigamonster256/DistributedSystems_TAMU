import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Tweet {
  private LongWritable datetime;
  private Text user;
  private Text title;
  private Text content;

  public Tweet(LongWritable datetime, Text user, Text title, Text content) {
    this.datetime = datetime;
    this.user = user;
    this.title = title;
    this.content = content;
  }

  public LongWritable getDatetime() {
    return datetime;
  }

  public Text getUser() {
    return user;
  }

  public Text getTitle() {
    return title;
  }

  public Text getContent() {
    return content;
  }
}