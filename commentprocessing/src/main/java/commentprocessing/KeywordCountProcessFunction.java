package commentprocessing;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class KeywordCountProcessFunction extends ProcessWindowFunction<RedditComment, String, String, TimeWindow> {
    public static final String KEYWORD = "trump";

    @Override
    public void process(String key, Context ctx, Iterable<RedditComment> iterable, Collector<String> collector) throws Exception {
        int mentions = 0;
        for (RedditComment comment : iterable) {
            List<String> words = Arrays.asList(comment.getMessage().toLowerCase().split(" "));

            mentions += words.stream().filter(w -> w.contains(KEYWORD)).count();
        }

        if (mentions > 0) {
            collector.collect(String.format("%s: %d", key, mentions));
        }
    }
}
