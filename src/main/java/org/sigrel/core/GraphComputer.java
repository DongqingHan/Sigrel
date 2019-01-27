package org.sigrel.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import com.alibaba.fastjson.JSON;

public class GraphComputer extends Reducer<Text, Text, Text, Text> {
    
    private Counter messageCounter = null;
    private Context context = null;
    private Vertex selfVertex = null;
    private List<Object> messages = new ArrayList<>();
    private List<Edge> outEs = new ArrayList<Edge>();
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        this.messageCounter = context.getCounter("Message-Counter", "count");
        this.context = context;
        this.messages.clear();
        this.outEs.clear();
    }
    
    /**
     * User defined function to be applied on each vertex.
     */
    public void compute(Text vertexId) {
        // [TODO]
        ;
    }
    
    /**
     * Get all the incoming messages.
     */
    public Iterator<Object> inMessages(){
        return this.messages.iterator();
    }
    
    /**
     * Sending message to vertex.
     * @param vertexId: vertex identifier.
     * @param value: message value in Text format.
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void sendMessage(Text vertexId, Object value)
            throws IOException, InterruptedException {
        // [TODO] not safe using json
        this.context.write(vertexId, new Text(valueConstruct(ValueType.MESSAGE, value)));
        this.messageCounter.increment(1);
    }
    
    /**
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // STEP 1: prepare vertex, out edges and in-messages.
        for (Text vl: values) {
            // TODO using JSON is not safe
            Map<String, Object> value = valueParse(vl.toString());
            if (value.get(Constants.INFOTYPE).equals(ValueType.MESSAGE)) {
                this.messages.add(value.get(Constants.DATA));
                
            } else if (value.get(Constants.INFOTYPE).equals(ValueType.EDGE)) {
                // TODO this is not correct!!!
                this.outEs.add((Edge) value.get(Constants.DATA));
                
            } else if (value.get(Constants.INFOTYPE).equals(ValueType.VERTEX)) {
                // 
                this.selfVertex = new Vertex(value.get(Constants.DATA).toString());
                
            } else {
                throw new IOException("invalide data type");
            }
        }
        // STEP 2: invoke user defined processing function, which may emit new message and
        // and make modification on vertex and edges.
        // Compute(Text vertexId, Iterable<Text> messages, Iterable<Edge> edges) may be better
        this.compute(key);
        
        // STEP 3: emit vertex and out-going edges with possible modified values.
        for (Edge edge: outEs) {
            context.write(key, new Text(valueConstruct(ValueType.EDGE, edge)));
        }
        context.write(key, new Text(valueConstruct(ValueType.VERTEX, this.selfVertex)));
    }
    
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        this.messageCounter = null;
        this.context = null;
        this.messages.clear();
        this.outEs.clear();
    }
    
    /**
     * @param vt
     * @param obj
     * @return
     */
    private static String valueConstruct(ValueType vt, Object obj) {
        // TODO obj may not be json serializable!
        Map<String, Object> message = new HashMap<String, Object>();
        message.put(Constants.INFOTYPE, vt);
        message.put(Constants.DATA, obj.toString());
        return JSON.toJSONString(message);
    }
    /**
     * @param jsonString
     * @return
     */
    private static Map<String, Object> valueParse(String jsonString) {
        return JSON.parseObject(jsonString);
    }
}
