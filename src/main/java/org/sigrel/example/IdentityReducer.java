package org.sigrel.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class IdentityReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> results = new ArrayList<String>();
        values.forEach(it -> { results.add(it.toString()); } );
        context.write(key, new Text(String.join(",", results)));
    }
}
