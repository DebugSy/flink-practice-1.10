package com.flink.demo.cases;

import java.sql.Time;
import java.sql.Timestamp;

public class Solution {

    public static void main(String[] args) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.err.println(timestamp);

        Timestamp timestamp1 = Timestamp.valueOf("2020-11-25 11:54:18.9");
        System.err.println(timestamp1);
    }

    public String convert(String s, int numRows) {
        StringBuilder sb = new StringBuilder();
        int len = s.length();
        int index = 0;
        int start = 0;
        int pre = 0;
        boolean flag = false;
        int max = (numRows - 1) * 2;
        if (max == 0) {
            max = 1;
        }
        for (int i = 0; i < len; i++) {
            char c = s.charAt(index);
            sb.append(c);
            int j = (numRows - start - 1) * 2;
            if (j == 0) {
                j = max;
            }
            if (j != max) {
                if (flag) {
                    j = max - pre;
                    flag = false;
                } else {
                    pre = j;
                    flag = true;
                }
            }
            if (index + j >= len) {
                index = ++start;
                flag = false;
            } else {
                index = index + j;
            }
        }
        return sb.toString();
    }
}
