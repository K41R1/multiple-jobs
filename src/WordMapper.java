import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private final Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = Collections.list(new StringTokenizer(line))
                .stream()
                .map(token -> (String)token)
                .map(String::toLowerCase)
                .map(word -> word.replaceAll("[^A-Z-a-z]",""))
                .filter(word -> word.length() > 3)
                .toArray(String[]::new);
        for (String word: words) {
            text.set(word);
            context.write(text, one);
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);
        while (context.nextKeyValue()) {
            this.map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        this.cleanup(context);
    }
}
