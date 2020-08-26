package commentprocessing;

public class RedditComment {
    private final String message;
    private final String author;
    private final String subreddit;
    private final long time;

    public RedditComment(String message, String author, String subreddit, long time) {
        this.message = message;
        this.author = author;
        this.subreddit = subreddit;
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    public String getSubreddit() {
        return subreddit;
    }

    public String getAuthor() {
        return author;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.getAuthor(), this.getMessage());
    }
}
