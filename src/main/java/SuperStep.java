
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.sigrel.core.COMPUTER_COUNTER;
import org.sigrel.core.Constants;
import org.sigrel.core.GraphReader;
import org.sigrel.core.GraphWriter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class SuperStep extends Configured implements Tool {
    /**
     * Run super step as:
     * GraphReader -> GraphComputer ->
     *              Identity Mapper -> GraphComputer ->
     *                                      ...
     *                               Identity Mapper -> GraphComputer ->
     *                                                Identity Mapper -> GraphWriter
     */
    public int run(String[] args) throws Exception {
        String input_path_str = null;
        String output_path_str = null;
        boolean completed = false;
        boolean final_iteration = false;
        int iteration = 0;
        for ( ; ; ++iteration) {
            Job job = Job.getInstance(getConf());
            Configuration conf = job.getConfiguration();
            // Set super step number to be checked in map or reduce tasks.
            conf.setInt(Constants.ITERATION, iteration);
            
            // Adjust input/output path and Mapper/Reducer class in each super step.
            if ( 0 == iteration ) {
                // first iteration, check startup arguments.
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if (otherArgs.length != 2) {
                    System.err.println("Usage: SuperStep <intput path> <output path>");
                    return -1;
                }
                input_path_str = otherArgs[0];
                output_path_str = otherArgs[1];
                
                Path in_path = new Path(input_path_str);
                Path out_path = new Path(output_path_str + String.valueOf(iteration));
                FileInputFormat.setInputPaths(job, in_path);
                FileOutputFormat.setOutputPath(job, out_path);
        
                job.setMapperClass(GraphReader.class);
                job.setReducerClass((Class<? extends Reducer>) Class.forName("org.sigrel.example.degree.DegreeCounter"));
                
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                
                job.setNumReduceTasks(10);
                
            } else if (final_iteration) {
                Path in_path = new Path(output_path_str + String.valueOf(iteration - 1));
                Path out_path = new Path(output_path_str);
                
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                KeyValueTextInputFormat.setInputPaths(job, in_path);
                FileOutputFormat.setOutputPath(job, out_path);
                
                job.setMapperClass(Mapper.class);
                job.setReducerClass(GraphWriter.class);
                
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                
                job.setNumReduceTasks(20);
            
            } else {
                Path in_path = new Path(output_path_str + String.valueOf(iteration - 1));
                Path out_path = new Path(output_path_str + String.valueOf(iteration));
                
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                KeyValueTextInputFormat.setInputPaths(job, in_path);
                FileOutputFormat.setOutputPath(job, out_path);
        
                job.setMapperClass(Mapper.class);
                job.setReducerClass((Class<? extends Reducer>) Class.forName("org.sigrel.example.degree.DegreeCounter"));
                
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                
                job.setNumReduceTasks(20);
            }
            
            completed =  job.waitForCompletion(true);
            
            // checkout terminate condition.
            if (!completed) {
                break;
            } else if (!final_iteration) {
                // check message number.
                // [TODO] will job be renewed during every submition???
                final_iteration = job.getCounters().findCounter(COMPUTER_COUNTER.MESSAGE_NUMBER).getValue() == 0;
            } else {
                break;
            }
        }
        // delete intermediate output directories.
        for (int i = 0; i <= iteration; ++i) {
            Job job = Job.getInstance(getConf());
            Configuration conf = job.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(new Path(output_path_str + String.valueOf(i)))) {
                fs.delete(new Path(output_path_str + String.valueOf(i)), true);
            }
        }

        return completed ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new SuperStep(), args);
        System.exit(res);
    }
}
