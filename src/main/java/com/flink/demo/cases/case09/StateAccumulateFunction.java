package com.flink.demo.cases.case09;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.*;
import java.time.temporal.TemporalAdjusters;

@Slf4j
public class StateAccumulateFunction extends KeyedProcessFunction<Tuple, Row, Row> {

    private ValueState<Row> valueState;

    private ValueState<Long> curTimeOfState;

    private final String timeOffsetMode;

    private final Long timeOffset;

    public StateAccumulateFunction(String timeOffsetMode, Long timeOffset) {
        this.timeOffsetMode = timeOffsetMode;
        this.timeOffset = timeOffset;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Row> valueInfo = TypeInformation.of(Row.class);
        ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("acc_state", valueInfo);
        this.valueState = getRuntimeContext().getState(stateDescriptor);

        TypeInformation<Long> curTimeStateInfo = TypeInformation.of(Long.class);
        ValueStateDescriptor<Long> curTimeStateDescriptor = new ValueStateDescriptor("current_time_of_state", curTimeStateInfo);
        this.curTimeOfState = getRuntimeContext().getState(curTimeStateDescriptor);
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
        Long timestamp = ctx.timestamp();
        Long currentTime = curTimeOfState.value();
        if (currentTime == null) {
            Long stateClearTime = executeStateClearTime(timestamp);
            log.info("Init curTimeOfState with value {}", stateClearTime);
            curTimeOfState.update(stateClearTime);
        } else {
            boolean needClear = needClear(currentTime, timestamp);
            log.info("Got curTimeOfState value {}, timestamp {}. isn't need clear: {}, current key: {}", currentTime, timestamp, needClear,
                    ctx.getCurrentKey());
            if (needClear) {
                Long stateClearTime = executeStateClearTime(timestamp);
                log.info("Update curTimeOfState with value {}, clear state", stateClearTime);
                curTimeOfState.update(stateClearTime);
                valueState.clear();
            }
        }

        Row cacheState = this.valueState.value();
        log.info("The cache state of key {} is  {}", ctx.getCurrentKey(), cacheState);
        Row accRow = Row.project(value, new int[]{0});
        if (cacheState != null) {
            for (int i = 0; i < accRow.getArity(); i++) {
                long cnt1 = Long.parseLong(accRow.getField(i).toString());
                long cnt2 = Long.parseLong(cacheState.getField(i).toString());
                long result = cnt1 + cnt2;
                log.info("{} + {} = {}", cnt1 ,cnt2 , result);
                accRow.setField(i, result);
            }
        }
        log.info("Update cache state of key {} with {}", ctx.getCurrentKey(), accRow);
        this.valueState.update(accRow);
        Row result = Row.join(value, accRow);
        out.collect(result);
    }

    /**
     * 根据curTimeOfState与timestamp比较，判断是否需要清理状态
     *
     * @param curTimeOfState
     * @param timestamp
     * @return
     */
    private boolean needClear(Long curTimeOfState, Long timestamp) {
        if (curTimeOfState < timestamp) {
            return true;
        }
        return false;
    }

    /**
     * 计算状态清理时间
     *
     * @param timestamp
     * @return
     */
    private Long executeStateClearTime(Long timestamp) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
        switch (timeOffsetMode) {
            case "M":
                LocalDateTime lastDay = localDateTime.with(TemporalAdjusters.lastDayOfMonth());
                LocalDate localDateOfMouth = lastDay.toLocalDate();
                LocalDateTime maxTimeOfMouth = LocalDateTime.of(localDateOfMouth, LocalTime.MAX);
                Instant maxInstantOfMouth = maxTimeOfMouth.plusMonths(timeOffset).atZone(zone).toInstant();
                return maxInstantOfMouth.toEpochMilli();
            case "d":
                LocalDate localDateOfDay = localDateTime.toLocalDate();
                LocalDateTime maxTimeOfDay = LocalDateTime.of(localDateOfDay, LocalTime.MAX);
                Instant maxInstantOfDay = maxTimeOfDay.plusDays(timeOffset).atZone(zone).toInstant();
                return maxInstantOfDay.toEpochMilli();
            case "H":
                LocalDateTime maxTimeOfHour = localDateTime.withMinute(59).withSecond(59).withNano(999_999_999);
                Instant maxInstantOfHour = maxTimeOfHour.plusHours(timeOffset).atZone(zone).toInstant();
                return maxInstantOfHour.toEpochMilli();
            case "m":
                LocalDateTime maxTimeOfMinute = localDateTime.withSecond(59).withNano(999_999_999);
                Instant maxInstantOfMinute = maxTimeOfMinute.plusMinutes(timeOffset).atZone(zone).toInstant();
                return maxInstantOfMinute.toEpochMilli();
            default:
                throw new RuntimeException("Not supported mode " + timeOffsetMode);
        }
    }
}
