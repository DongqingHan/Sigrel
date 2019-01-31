package org.sigrel.example.degree;

import java.io.IOException;
import java.util.List;

import org.sigrel.core.Edge;
import org.sigrel.core.GraphComputer;
import org.sigrel.core.Message;
import org.sigrel.core.Vertex;

public class DegreeCounter extends GraphComputer<Integer, String, Integer> {
    protected void compute(Vertex<Integer> vertex, List<Edge<String>> outEs, List<Message<Integer>> messages)
            throws IOException, InterruptedException {
        
        if (0 == this.getIteration()) {
            for (Edge<String> outE: outEs) {
                this.sendMessage(outE.getId(), new Message<Integer>(1));
            }
            
        } else if (1 == this.getIteration()) {
            int inDegree = 0;
            for (Message<Integer> ms : messages) {
                inDegree += ms.getValue();
            }
            vertex.setValue(inDegree + outEs.size());
        } else {
            // [TODO] last iteration
            
        }
        
    }
}
