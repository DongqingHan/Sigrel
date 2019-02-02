package org.sigrel.core;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphReader extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\u0001");
        String from_vertex = tokens[1];
        String to_vertex = tokens[2];
        
        context.write(new Text(from_vertex),
                new Text(GraphComputer.valueConstruct(Constants.CONTENT_TYPE_VERTEX, new Vertex<Integer>(from_vertex, null))));
        context.write(new Text(from_vertex),
                new Text(GraphComputer.valueConstruct(Constants.CONTENT_TYPE_EDGE, new Edge<String>(to_vertex, null))));
    }
}
