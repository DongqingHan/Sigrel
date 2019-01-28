package org.sigrel.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import com.alibaba.fastjson.JSON;

public abstract class GraphComputer<V, E, M> extends Reducer<Text, Text, Text, Text> {
    
    private Counter messageCounter = null;
    private Context context = null;
    private Vertex<V> selfVertex = null;
    private List<Message<M>> messages = new ArrayList<>();
    private List<Edge<E>> outEs = new ArrayList<>();
    
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
    abstract void compute(Vertex<V> vertex, List<Edge<E>> outEs, List<Message<M>> messages);
    
    /**
     * Get all the incoming messages.
     */
    public Iterator<Message<M>> inMessages(){
        return this.messages.iterator();
    }
    
    /**
     * Sending message to vertex.
     * @param vertexId: vertex identifier.
     * @param value: message value in Text format.
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void sendMessage(Text vertexId, Message<M> value)
            throws IOException, InterruptedException {
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
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // STEP 1: prepare vertex, out edges and in-messages.
        for (Text vl: values) {
            JsonConstruct<Object> value = valueParse(vl.toString());
            
            if (value.getValueType().equals(ValueType.MESSAGE)) {
                this.messages.add((Message<M>) value.getValue());
                
            } else if (value.getValueType().equals(ValueType.EDGE)) {
                this.outEs.add((Edge<E>) value.getValue());
                
            } else if (value.getValueType().equals(ValueType.VERTEX)) {
                this.selfVertex = (Vertex<V>) value.getValue();
            } else {
                // not implement errror
                throw new IOException("invalide data type");
            }
        }
        // STEP 2: invoke user defined processing function, which may emit new message and
        // and make modification on vertex and edges.
        // Compute(Text vertexId, Iterable<Text> messages, Iterable<Edge> edges) may be better
        this.compute(this.selfVertex, this.outEs, this.messages);
        
        // STEP 3: emit vertex and out-going edges with possible modified values.
        for (Edge<E> edge: this.outEs) {
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
     * @param <T>
     * @param vt
     * @param obj
     * @return
     */
    private static <T> String valueConstruct(ValueType vt, Element<T> elm) { 
        JsonConstruct<T> resJson = new JsonConstruct<T>(vt, elm);
        return JSON.toJSONString(resJson);
    }
    /**
     * @param <T>
     * @param jsonString
     * @return
     */
    private static <T> JsonConstruct<T> valueParse(String jsonString) {
        return JSON.parseObject(jsonString, JsonConstruct.class);
    }
}
