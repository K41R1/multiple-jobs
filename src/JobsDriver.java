import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobsDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage : [input] [output]");
            System.exit(1);
        }
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        Path out1 = new Path(args[1] + "/temp");
        Path out2 = new Path(args[1] + "/final");
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("word count");

        job.setJarByClass(JobsDriver.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordSumReducer.class);
        job.setCombinerClass(WordSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out1);

        Job job1 = Job.getInstance(conf);
        job1.setJobName("reverse and sort");
        job1.setJarByClass(JobsDriver.class);
        /*
        job1.setMapperClass(WordReverseMapper.class);
        job1.setReducerClass(SortByFrequencyReducer.class);
         */
        Configuration map2Conf, map1Conf, reduceConf;
        map1Conf = map2Conf = reduceConf = new Configuration(false);
        ChainMapper.addMapper(job1, NormalizeLineMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, map1Conf);
        ChainMapper.addMapper(job1, WordReverseMapper.class, Text.class, IntWritable.class, IntWritable.class, Text.class, map2Conf);
        ChainReducer.setReducer(job1, SortByFrequencyReducer.class, IntWritable.class, Text.class, IntWritable.class, Text.class, reduceConf);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1, out1);
        FileOutputFormat.setOutputPath(job1, out2);
        ControlledJob controlledJob = new ControlledJob(conf);
        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob.setJob(job);
        controlledJob1.setJob(job1);
        controlledJob1.addDependingJob(controlledJob);

        JobControl jobControl = new JobControl("word jobs");
        jobControl.addJob(controlledJob);
        jobControl.addJob(controlledJob1);
        jobControl.run();
        Thread jobThread = new Thread(jobControl);
        jobThread.setDaemon(true);
        jobThread.run();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(
                ToolRunner.run(new JobsDriver(), args)
        );
    }
}
