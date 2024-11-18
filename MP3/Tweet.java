import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class Tweet {
  private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);

  private LocalDateTime datetime;
  private String username;
  private String content;

  public Tweet(String datetime, String username, String content) {
    try {
      this.datetime = LocalDateTime.parse(datetime, DATE_TIME_FORMATTER);
    } catch (DateTimeParseException e) {
      System.err.println("Invalid datetime format: " + datetime);
      System.exit(1);
    }
    this.username = username;
    this.content = content;
  }

  public LocalDateTime getDateTime() {
    return datetime;
  }

  public String getUsername() {
    return username;
  }

  public String getContent() {
    return content;
  }

  @Override
  public String toString() {
    return datetime + "\n" + username + "\n" + content;
  }
}