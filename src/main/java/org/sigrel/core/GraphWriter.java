package org.sigrel.core;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;


public class GraphWriter extends GraphComputer<Integer, String, Integer> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // STEP 1: prepare vertex, out edges and in-messages.
        for (Text vl: values) {
            Map<String, Object> value = valueParse(vl.toString());
            if (Constants.CONTENT_TYPE_VERTEX.equals(value.get(Constants.CONTENT_TYPE))) {
                //this.selfVertex = (Vertex<Integer>) value.getValue();
                Vertex<Integer> vertex = (Vertex<Integer>) value.get(Constants.CONTENT_VALUE);
                context.write(key, new Text(java.lang.String.valueOf(vertex.getValue())));
            } else {
                ;
            }
        }
    }
    
}
