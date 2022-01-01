package com.jinx.model;

import java.io.Serializable;

import scala.math.Ordered;

public class movieSort implements  Serializable,Ordered<movieSort> {
    private int key;
    private int value;

    @Override
    public String toString() {
        return "movieSort{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public movieSort(int key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compare(movieSort that) {

        if(this.key!=that.key){
            return this.key-that.key;
        }else {
            return this.value-that.value;
        }
    }



    @Override
    public int compareTo(movieSort that) {
        if(this.key!=that.key){
            return this.key-that.key;
        }else {
            return this.value-that.value;
        }
    }
}
