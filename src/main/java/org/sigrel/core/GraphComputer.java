package org.sigrel.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

public class GraphComputer<V, E, M> extends Reducer<Text, Text, Text, Text> {
    
    private int iteration;
    private Counter messageCounter = null;
    private Context context = null;
    protected Vertex<V> selfVertex = null;
    protected List<Message<M>> messages = null;
    protected List<Edge<E>> outEs = null;
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        this.setIteration(context.getConfiguration().getInt(Constants.ITERATION, 0));
        this.messageCounter = context.getCounter(COMPUTER_COUNTER.MESSAGE_NUMBER);
        this.context = context;
        this.messages = new ArrayList<Message<M>>();
        this.outEs = new ArrayList<Edge<E>>();
    }
    
    public int getIteration() {
        return iteration;
    }

    private void setIteration(int iteration) {
        this.iteration = iteration;
    }
    
    /** User defined function to be applied on each vertex.
     * @param vertex vertex will not be null.
     * @param outEs out edge list from this vertex, which may be empty.
     * @param messages messages sending to this vertex from in edges, which may be empty.
     * @throws InterruptedException 
     * @throws IOException 
     */
    protected void compute(Vertex<V> vertex, List<Edge<E>> outEs, List<Message<M>> messages)
            throws IOException, InterruptedException { }

    /**
     * @param key vertex identifier of String type.
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // STEP 0: reset executing environment.
        this.messages.clear();
        this.outEs.clear();
        this.selfVertex = null;
        
        // STEP 1: prepare vertex, out edges and in-messages.
        for (Text vl: values) {
            Map<String, Object> value = valueParse(vl.toString());
            if (Constants.CONTENT_TYPE_MESSAGE.equals(value.get(Constants.CONTENT_TYPE))) {
                this.messages.add((Message<M>) value.get(Constants.CONTENT_VALUE));
                
            } else if (Constants.CONTENT_TYPE_EDGE.equals(value.get(Constants.CONTENT_TYPE))) {
                this.outEs.add((Edge<E>) value.get(Constants.CONTENT_VALUE));
                
            } else if (Constants.CONTENT_TYPE_VERTEX.equals(value.get(Constants.CONTENT_TYPE))) {
                this.selfVertex = (Vertex<V>) value.get(Constants.CONTENT_VALUE);
                
            } else {
                // not implement error
                throw new IOException("un-supported value type");
            }
//            A<Element<Object>, Object> value = valueParse(vl.toString());
//            if (value.getType().equals("MESSAGE")) {
//                this.messages.add((Message<M>) value.getValue());
//            } else if (value.getType().equals("EDGE")) {
//                this.outEs.add((Edge<E>) value.getValue());
//            } else if (value.getType().equals("VERTEX")) {
//                this.selfVertex = (Vertex<V>) value.getValue();
//            } else {
//                throw new IOException("invalide data type");
//            }
            
        }
        
        // [TODO] Could vertex disappeared? or we have to create a new vertex?
        if (null == this.selfVertex) { return; }
        
        // STEP 2: invoke user defined processing function, which may emit new message and
        // and make modification on vertex and edges.
        // [TODO] Compute(Text vertexId, Iterable<Text> messages, Iterable<Edge> edges) may be better
        this.compute(this.selfVertex, this.outEs, this.messages);
        
        // STEP 3: emit vertex and out-going edges with possible modified values.
        for (Edge<E> edge: this.outEs) {
            context.write(key, new Text(valueConstruct(Constants.CONTENT_TYPE_EDGE, edge)));
        }
        context.write(key, new Text(valueConstruct(Constants.CONTENT_TYPE_VERTEX, this.selfVertex)));
    }
    
    /**
     * Sending message to vertex.
     * @param vertexId: vertex identifier.
     * @param value: message value in Text format.
     * @throws InterruptedException 
     * @throws IOException 
     */
    protected void sendMessage(String vertexId, Message<M> value)
            throws IOException, InterruptedException {
        // [TODO] will this.context be identical to context in reduce method??? 
        this.context.write(new Text(vertexId), new Text(valueConstruct(Constants.CONTENT_TYPE_MESSAGE, value)));
        this.messageCounter.increment(1);
    }
    
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        this.messageCounter = null;
        this.context = null;
        this.messages = null;
        this.outEs = null;
    }
    
    /**
     * @param <T>
     * @param vt
     * @param obj
     * @return
     */
    public static <T> String valueConstruct(String vt, Element<T> elm) { 
        Map<String, String> mp = new HashMap<>();
        mp.put(Constants.CONTENT_TYPE, vt);
        mp.put(Constants.CONTENT_VALUE, JSON.toJSONString(elm));
        return JSON.toJSONString(mp);
    }
    
    /**
     * @param <T>
     * @param jsonString
     * @return
     */
    public Map<String, Object> valueParse(String jsonString) {
        Map<String, Object> out= new HashMap<>();
        Map<String, String> mp = JSON.parseObject(jsonString, Map.class);
        String vt = mp.get(Constants.CONTENT_TYPE);
        out.put(Constants.CONTENT_TYPE, vt);
        switch (vt) {
        case Constants.CONTENT_TYPE_MESSAGE:
            out.put(Constants.CONTENT_VALUE, JSON.parseObject( mp.get(Constants.CONTENT_VALUE), new TypeReference<Message<M>>() {}));
            break;
        case Constants.CONTENT_TYPE_EDGE:
            out.put(Constants.CONTENT_VALUE, JSON.parseObject( mp.get(Constants.CONTENT_VALUE), new TypeReference<Edge<E>>() {}));
            break;
        case Constants.CONTENT_TYPE_VERTEX:
            out.put(Constants.CONTENT_VALUE, JSON.parseObject( mp.get(Constants.CONTENT_VALUE), new TypeReference<Vertex<V>>() {}));
            break;
        default:
            break;
        }
        return out;
    }

//    public static class  A<S extends Element<T>, T>{
//        private String type;
//        private S value;
//        public String getType() {
//            return type;
//        }
//        public void setType(String type) {
//            this.type = type;
//        }
//        public S getValue() {
//            return value;
//        }
//        public void setValue(S value) {
//            this.value = value;
//        }
//        
//    }
//    
//    public static <S extends Element<T>, T> String valueConstruct2(String vt, S elm) { 
//        A<S, T> a = new A<S, T>();
//        a.setType(vt);
//        a.setValue(elm);
//        return JSON.toJSONString(a);
//    }
//    
//    public <S extends Element<T>, T> A<S, T> valueParse2(String jsonString) {
//        return JSON.parseObject(jsonString, new TypeReference<A<S, T>>(){});
//    }
}
