package org.sigrel.core;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

public class Element<T> {
    private String id;
    private T value;
    
    public Element(String id, T value) {
        super();
        this.id = id;
        this.value = value;
    }

    public Element() {
        super();
        this.id = null;
        this.value = null;
        // TODO Auto-generated constructor stub
    }

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

}
