package org.sigrel.example;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddOne extends Mapper<Object, Text, Text, Text> {
    // recent super step number.
    protected int iteration;
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        iteration = context.getConfiguration().getInt("iteration", 0);
    }
    
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        context.getCounter("Message-Counter", "count").increment(1);
        context.write(new Text(String.valueOf(iteration)), new Text(value.toString() + String.valueOf(iteration)));
    }
}
