package com.flink.demo.cases.case12;


import org.apache.flink.table.functions.TableFunction;

public class ExplodeUDTF extends TableFunction<String> {

    public void eval(String column, String separator) {
        String[] strings = column.split(separator);
        for (String str : strings) {
            collect(str);
        }
    }

}