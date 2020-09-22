package com.flink.demo.cases.case12;

import org.apache.flink.table.functions.ScalarFunction;

public class RowKeyGen extends ScalarFunction {

    public String eval(String userId) {
        return "HBase_Table_" + userId;
    }

}
