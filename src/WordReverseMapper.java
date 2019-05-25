import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordReverseMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

    private final Text v = new Text();
    private final IntWritable k = new IntWritable();

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
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
