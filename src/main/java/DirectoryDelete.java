
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class DirectoryDelete extends Configured implements Tool {
    /**
     */
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: DirectoryDelete <intput path>");
            return -1;
        }
        
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(otherArgs[0]))) {
            fs.delete(new Path(otherArgs[0]), true);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new DirectoryDelete(), args);
        System.exit(res);
    }
}
