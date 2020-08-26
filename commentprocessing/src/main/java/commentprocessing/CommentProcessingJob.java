package commentprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class CommentProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        DataStream<RedditComment> comments = env
                .addSource(new FlinkKafkaConsumer<>("reddit-comments", new JsonNodeDeserializationSchema(), properties))
                .map(jsonNode -> new RedditComment(jsonNode.get("message").asText(), jsonNode.get("author").asText(),
                        jsonNode.get("subreddit").asText(), jsonNode.get("time").asLong()))
                .name("comments");

        comments.addSink(new PrintSinkFunction<>());

        env.execute("Comment Processing");
    }
}
