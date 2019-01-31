package org.sigrel.core;


import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.sigrel.core.GraphComputer.A;


public class GraphWriter extends GraphComputer<Integer, String, Integer> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // STEP 1: prepare vertex, out edges and in-messages.
        for (Text vl: values) {
            Map<String, Object> value = valueParse(vl.toString());
            if (value.get("type").equals("VERTEX")) {
                //this.selfVertex = (Vertex<Integer>) value.getValue();
                Vertex<Integer> vertex = (Vertex<Integer>) value.get("value");
                context.write(key, new Text(java.lang.String.valueOf(vertex.getValue())));
            } else {
                ;
            }
        }
    }
    
}
