package com.flink.demo.cases.case28;

public class T1 implements Runnable{

    public String person;

    public T1(String person) {
        this.person = person;
    }

    @Override
    public void run() {
        System.out.println("set person name to name2");
        this.person = "name2";
    }
}
