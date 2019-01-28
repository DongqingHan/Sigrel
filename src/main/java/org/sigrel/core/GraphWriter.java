package org.sigrel.core;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GraphWriter extends Mapper<Text, Text, NullWritable, Text> {
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}
