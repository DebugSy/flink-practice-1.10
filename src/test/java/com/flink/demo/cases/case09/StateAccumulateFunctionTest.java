package com.flink.demo.cases.case09;

import junit.framework.TestCase;
import org.junit.Test;

import java.time.*;
import java.time.temporal.TemporalAdjusters;

public class StateAccumulateFunctionTest extends TestCase {

    @Test
    public void testExecuteStateClearTimeOfMouth() {
        Long time = executeStateClearTime(1598005382974L, "M", 0L);
        assertEquals(1598889599999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfDay() {
        Long time = executeStateClearTime(1598005382974L, "d", 0L);
        assertEquals(1598025599999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfHour() {
        Long time = executeStateClearTime(1598005382974L, "H", 0L);
        assertEquals(1598007599999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfMinute() {
        Long time = executeStateClearTime(1598005382974L, "m", 0L);
        assertEquals(1598005439999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfMouth_offset_6() {
        Long time = executeStateClearTime(1598005382974L, "M", 6L);
        assertEquals(1614527999999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfDay_offset_6() {
        Long time = executeStateClearTime(1598005382974L, "d", 6L);
        assertEquals(1598543999999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfHour_offset_6() {
        Long time = executeStateClearTime(1598005382974L, "H", 6L);
        assertEquals(1598029199999L, time.longValue());
    }

    @Test
    public void testExecuteStateClearTimeOfMinute_offset_6() {
        Long time = executeStateClearTime(1598005382974L, "m", 6L);
        assertEquals(1598005799999L, time.longValue());
    }


    /**
     * 计算状态清理时间
     * @param timestamp
     * @return
     */
    public Long executeStateClearTime(Long timestamp, String mode, Long offset) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
        switch (mode) {
            case "M":
                LocalDateTime lastDay = localDateTime.with(TemporalAdjusters.lastDayOfMonth());
                LocalDate localDateOfMouth = lastDay.toLocalDate();
                LocalDateTime maxTimeOfMouth = LocalDateTime.of(localDateOfMouth, LocalTime.MAX);
                Instant maxInstantOfMouth = maxTimeOfMouth.plusMonths(offset).atZone(zone).toInstant();
                return maxInstantOfMouth.toEpochMilli();
            case "d":
                LocalDate localDateOfDay = localDateTime.toLocalDate();
                LocalDateTime maxTimeOfDay = LocalDateTime.of(localDateOfDay, LocalTime.MAX);
                Instant maxInstantOfDay = maxTimeOfDay.plusDays(offset).atZone(zone).toInstant();
                return maxInstantOfDay.toEpochMilli();
            case "H":
                LocalDateTime maxTimeOfHour = localDateTime.withMinute(59).withSecond(59).withNano(999_999_999);
                Instant maxInstantOfHour = maxTimeOfHour.plusHours(offset).atZone(zone).toInstant();
                return maxInstantOfHour.toEpochMilli();
            case "m":
                LocalDateTime maxTimeOfMinute = localDateTime.withSecond(59).withNano(999_999_999);
                Instant maxInstantOfMinute = maxTimeOfMinute.plusMinutes(offset).atZone(zone).toInstant();
                return maxInstantOfMinute.toEpochMilli();
            default:
                throw new RuntimeException("Not supported mode " + mode);
        }
    }
}