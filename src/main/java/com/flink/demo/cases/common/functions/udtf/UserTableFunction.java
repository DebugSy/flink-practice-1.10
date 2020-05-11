package com.flink.demo.cases.common.functions.udtf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by DebugSy on 2019/8/8.
 *
 * Table Function
 * what: 模拟静态表，主要是做流表与维度表的join
 * why: 根据用户名称查询维度表中的用户信息
 * how: 将其注册成TableFunction，使用function name直接调用
 * eg:
 */
public class UserTableFunction extends TableFunction<Row> {

    private static final Logger logger = LoggerFactory.getLogger(UserTableFunction.class);

    private Map<String, Row> users = new HashMap<>();

    @Override
    public void open(FunctionContext context) throws Exception {
        for (int i = 0; i < 10; i++) {
            Row r = new Row(3);
            String key = "用户" + (char) ('A' + i);
            r.setField(0, key);
            r.setField(1, "地址" + (char) ('A' + i));
            r.setField(2, "1881041220" + i);
            logger.info("Added row {}", r);
            users.put(key, r);
        }
    }

    public void eval(String username) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (users.containsKey(username)) {
            Row row = users.get(username);
            logger.info("get row {} with key {}", row, username);
            collect(row);
        } else {
            logger.info("can't find key:{}", username);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        logger.info("Get result type");
        return Types.ROW(Types.STRING(), Types.STRING(), Types.STRING());
    }
}
