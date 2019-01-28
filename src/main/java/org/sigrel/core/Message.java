package org.sigrel.core;

/**
 * @author handongqing01
 *
 * @param <T>
 */
public class Message<T> extends Element<T> {
    
    public Message(T value) {
        super(null, value);
        // TODO Auto-generated constructor stub
    }
    public Message(String id, T value) {
        super(id, value);
    }

}
