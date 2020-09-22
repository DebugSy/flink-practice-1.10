package com.flink.demo.cases.case10;

import org.junit.Test;

import java.time.*;
import java.time.temporal.TemporalAdjusters;

public class LocalDateTimeTrainingTest{

    /*
    * 获取当前时间当天的最大值
    * */
    @Test
    public void testGetDayMaxTime() {
        long timestamp = System.currentTimeMillis();
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
        LocalDate localDate = localDateTime.toLocalDate();
        LocalDateTime maxLocalDateTime = LocalDateTime.of(localDate, LocalTime.MAX);
        Instant maxInstant = maxLocalDateTime.atZone(zone).toInstant();
        long toEpochMilli = maxInstant.toEpochMilli();
        System.out.println(toEpochMilli);
    }

    /*
     * 获取当前时间小时的最大值
     * */
    @Test
    public void testGetHourMaxTime() {
        long timestamp = System.currentTimeMillis();
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zone);
        LocalDateTime maxTimeOfHour = localDateTime.withMinute(59).withSecond(59).withNano(999_999_999);
        Instant maxInstant = maxTimeOfHour.atZone(zone).toInstant();
        System.out.println(maxInstant.atZone(zone));
        long toEpochMilli = maxInstant.toEpochMilli();
        System.out.println(toEpochMilli);
    }

    /*
     * 获取当前时间段的最大值
     * */
    @Test
    public void testGetMaxTimeOfPeriod() {
        LocalDate startDate = LocalDate.of(2015, 2, 20);
        LocalDate endDate = LocalDate.of(2017, 1, 15);
        Period period = Period.between(startDate, endDate);
        int days = period.getDays();
        System.out.println(days);
    }

    /*
     * 获取当前时间所在月的第一天与最后一天
     * */
    @Test
    public void testGetFirstAndLatestDay() {
        LocalDateTime date = LocalDateTime.of(2019,2,1, 0, 4);
        LocalDateTime firstDay = date.with(TemporalAdjusters.firstDayOfMonth());
        LocalDateTime lastDay = date.with(TemporalAdjusters.lastDayOfMonth());
        System.out.println("firstDay:" + firstDay);
        System.out.println("lastDay:" + lastDay);
    }



}