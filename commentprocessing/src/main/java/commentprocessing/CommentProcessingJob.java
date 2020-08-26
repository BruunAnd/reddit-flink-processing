package commentprocessing;

import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class CommentProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        DataStream<RedditComment> comments = env
                .addSource(new FlinkKafkaConsumer<>("reddit-comments", new JsonNodeDeserializationSchema(), properties))
                .map(CommentProcessingJob::mapToComment)
                .name("comments");

        DataStream<String> counts = comments
                .keyBy(RedditComment::getSubreddit)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new KeywordCountProcessFunction())
                .name("comment-counter");

        counts.print();

        env.execute("Comment Processing");
    }

    private static RedditComment mapToComment(ObjectNode jsonNode) {
        return new RedditComment(jsonNode.get("message").asText(), jsonNode.get("author").asText(),
                jsonNode.get("subreddit").asText(), jsonNode.get("time").asLong());
    }
}
