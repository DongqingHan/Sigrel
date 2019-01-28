package org.sigrel.core;

public class Element<T> {
    private String id;
    private T value;
    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public Element(String id, T value) {
        super();
        this.id = id;
        this.value = value;
    }
}
