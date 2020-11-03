package com.flink.demo.cases.case20;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Comparator;

public class RowColCompare implements Comparator<Row>, Serializable {

    private final int[] compareIdx;

    public RowColCompare(int[] compareIdx) {
        this.compareIdx = compareIdx;
    }

    @Override
    public int compare(Row o1, Row o2) {
        int i = 0;
        while (i < compareIdx.length) {
            Comparable o1v = (Comparable) o1.getField(i);
            Comparable o2v = (Comparable) o2.getField(i);
            if (o1v.compareTo(o2v) > 0) {
                return 1;
            } else if (o1v.compareTo(o2v) < 0) {
                return -1;
            } else {
                i++;
            }
        }
        return 0;
    }
}
