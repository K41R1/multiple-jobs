import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NormalizeLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text v = new Text();
    private final IntWritable k = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        v.set(parts[0]);
        k.set(Integer.parseInt(parts[1]));
        context.write(v, k);
    }
}
