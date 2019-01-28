package org.sigrel.core;


public class JsonConstruct<T> {
    private ValueType valueType;
    private Element<T> value;
    //final private String jsonString;
    public JsonConstruct(ValueType valueType, Element<T> value) {
        super();
        this.valueType = valueType;
        this.value = value;
    }
    
    public ValueType getValueType() {
        return valueType;
    }
    
    /**[TODO] make this unchangeable
     * @param valueType
     */
    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }
    public Element<T> getValue() {
        return value;
    }
    
    /** [TODO] make this unchangeable
     * @param value
     */
    public void setValue(Element<T> value) {
        this.value = value;
    }

}
