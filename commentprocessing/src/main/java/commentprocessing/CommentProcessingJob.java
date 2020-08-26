package commentprocessing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class CommentProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        DataStream<RedditComment> comments = env
                .addSource(new FlinkKafkaConsumer<>("reddit-comments", new JsonNodeDeserializationSchema(), properties))
                .map(CommentProcessingJob::mapToComment)
                .name("comments");

        // Assign timestamps, multiplied by 1K to get milliseconds
        WatermarkStrategy<RedditComment> strategy = WatermarkStrategy
                .<RedditComment>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((comment, time) -> comment.getTime() * 1000);

        DataStream<String> counts = comments
                .assignTimestampsAndWatermarks(strategy)
                .keyBy(RedditComment::getSubreddit)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
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
