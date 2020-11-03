package com.flink.demo.cases.case18;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class FlinkCEPTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Integer> data = new ArrayList<>();
        data.addAll(Arrays.asList(10, 13, 15, 40, 40,  10, 6, 15, 40, 40,  10, 6, 15, 40, 40,  10, 13, 15, 40, 40));

        DataStream<Integer> input = env.addSource(new SourceFunction<Integer>() {

            boolean running = true;

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                int i = 0;
                while (running) {
                    Integer record = data.get(i);
                    ctx.collect(record);
                    log.info("emit {}", record);
                    i++;
                    if (i > data.size() - 1) {
                        running = false;
                        break;
                    }
                    if (i % 5 == 0) {
                        log.info("sleeping 5s");
                        Thread.sleep(1000 * 5);
                    } else {
                        log.info("sleeping 1s");
                        Thread.sleep(1000 * 1);
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(1);

        Pattern<Integer, ?> onesThenZero =
                Pattern.<Integer>begin("start", AfterMatchSkipStrategy.noSkip())
                        .where(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value > 8;
                            }
                        })
                        .or(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value == 6;
                            }
                        })
                        .timesOrMore(2)
                        .consecutive()
                        .greedy()
                        .until(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value == 15;
                            }
                        })

                        .followedBy("end")
                        .where(new SimpleCondition<Integer>() {
                            @Override
                            public boolean filter(Integer value) throws Exception {
                                return value == 40;
                            }
                        })
                        .times(2)
                        .consecutive()
                        .greedy();

        PatternStream<Integer> pattern = CEP.pattern(input, onesThenZero);

        pattern.process(new PatternProcessFunction<Integer, Object>() {
            @Override
            public void processMatch(Map<String, List<Integer>> match, Context ctx, Collector<Object> out) throws Exception {
                System.err.println(match);
            }
        });

        env.execute();
    }

}
