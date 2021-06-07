package com.example.kafkamonitor.utils;

public class MutableInt {
    int value = 0;

    public MutableInt(int startingValue) {
        value = startingValue;
    }

    public MutableInt() {
        this(0);
    }

    public void increment() { ++value; }
    public void increase(int inc) { value += inc; }
    public int getValue() { return value; }
}