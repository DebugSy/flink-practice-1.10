package com.flink.demo.cases;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.junit.Test;

public class CommonsMathTest {

    @Test
    public void testCalculatePercentile() {
        //求数组的百分位
        double[] values = new double[]{1, 2, 3, 4, 5};
        Percentile percentilesImpl = new Percentile().withNaNStrategy(NaNStrategy.FIXED);
        percentilesImpl.setData(values);
        double evaluate = percentilesImpl.evaluate(50);
        System.out.println(evaluate);
    }

}
