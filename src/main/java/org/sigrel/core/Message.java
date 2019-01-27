package org.sigrel.core;

import org.apache.hadoop.io.Text;

public class Message {
    private Text value;
    public Message(Text value) {
        this.setValue(value);
    }
    public Text getValue() {
        return value;
    }
    public void setValue(Text value) {
        this.value = value;
    }
}
