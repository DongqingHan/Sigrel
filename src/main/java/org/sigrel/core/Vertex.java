package org.sigrel.core;

public class Vertex {
    private String id = null;
    private Object value = null;
    public Vertex(String string) {
        // TODO Auto-generated constructor stub
        id = string;
    }
    public String id() {
        return id;
    }
    public void setValue(Object value) {
        this.value = value;
    }
    public Object getValue() {
        return this.value;
    }
    public String toString() {
        // TODO serialize vertex instance to string.
        return id;
    }
}
