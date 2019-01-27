
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import org.sigrel.example.AddOne;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class JobLoop extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        
        long num = 0;
        for (int i = 0; i < 3; ++i) {
            Job job = Job.getInstance(getConf());
            Configuration conf = job.getConfiguration();
            
            // Check startup arguments.
            String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
            if (otherArgs.length != 2) {
                System.err.println("Usage: JobLoop <intput path> <output path>");
                return -1;
            }
            
            // Set super step number to be used in map or reduce tasks.
            conf.setInt("iteration", (int) num);
            
            // Adjust input and output path in each super step.
            Path in_path = new Path(otherArgs[0]);
            Path out_path = new Path(otherArgs[1] + String.valueOf(i));
            FileInputFormat.setInputPaths(job, in_path);
            FileOutputFormat.setOutputPath(job, out_path);
    
            job.setMapperClass(AddOne.class);
            job.setNumReduceTasks(0);
            boolean completed =  job.waitForCompletion(true);
            
            // checkout terminate condition.
            if (!completed) {
                return 1;
            } else {
                num = job.getCounters().findCounter("Message-Counter", "count").getValue();
            }
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new JobLoop(), args);
        System.exit(res);
    }
}
