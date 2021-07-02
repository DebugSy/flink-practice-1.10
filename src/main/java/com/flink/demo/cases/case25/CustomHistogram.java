package com.flink.demo.cases.case25;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.UnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.moment.SecondMoment;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.ranking.NaNStrategy;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;

@Slf4j
public class CustomHistogram implements Histogram {

    private final CircularDoubleArray descriptiveStatistics;

    public CustomHistogram(int windowSize) {
        this.descriptiveStatistics = new CircularDoubleArray(windowSize);
    }


    @Override
    public void update(long value) {
        this.descriptiveStatistics.addValue(value);
    }

    @Override
    public long getCount() {
        return this.descriptiveStatistics.getElementsSeen();
    }

    @Override
    public HistogramStatistics getStatistics() {
        double[] unsortedArray = this.descriptiveStatistics.toUnsortedArray();
        CustomHistogramStatistics statistics = new CustomHistogramStatistics(unsortedArray);
        return statistics;
    }

    /**
     * Fixed-size array that wraps around at the end and has a dynamic start position.
     */
    static class CircularDoubleArray {
        private final double[] backingArray;
        private int nextPos = 0;
        private boolean fullSize = false;
        private long elementsSeen = 0;

        CircularDoubleArray(int windowSize) {
            this.backingArray = new double[windowSize];
        }

        synchronized void addValue(double value) {
            backingArray[nextPos] = value;
            ++elementsSeen;
            ++nextPos;
            if (nextPos == backingArray.length) {
                nextPos = 0;
                fullSize = true;
            }
        }

        synchronized double[] toUnsortedArray() {
            final int size = getSize();
            double[] result = new double[size];
            System.arraycopy(backingArray, 0, result, 0, result.length);
            return result;
        }

        private synchronized int getSize() {
            return fullSize ? backingArray.length : nextPos;
        }

        private synchronized long getElementsSeen() {
            return elementsSeen;
        }
    }
}

class CustomHistogramStatistics extends HistogramStatistics {

    private final CommonMetricsSnapshot statisticsSummary = new CommonMetricsSnapshot();

    public CustomHistogramStatistics(double[] values) {
        statisticsSummary.evaluate(values);
    }

    @Override
    public double getQuantile(double quantile) {
        return statisticsSummary.getPercentile(quantile * 100);
    }

    @Override
    public long[] getValues() {
        return Arrays.stream(statisticsSummary.getValues()).mapToLong(i -> (long) i).toArray();
    }

    @Override
    public int size() {
        return (int) statisticsSummary.getCount();
    }

    @Override
    public double getMean() {
        return statisticsSummary.getMean();
    }

    @Override
    public double getStdDev() {
        return statisticsSummary.getStandardDeviation();
    }

    @Override
    public long getMax() {
        return (long) statisticsSummary.getMax();
    }

    @Override
    public long getMin() {
        return (long) statisticsSummary.getMin();
    }

    private static class CommonMetricsSnapshot implements UnivariateStatistic {
        private long count = 0;
        private double min = Double.NaN;
        private double max = Double.NaN;
        private double mean = Double.NaN;
        private double stddev = Double.NaN;
        private Percentile percentilesImpl = new Percentile().withNaNStrategy(NaNStrategy.FIXED);

        @Override
        public double evaluate(final double[] values) throws MathIllegalArgumentException {
            return evaluate(values, 0, values.length);
        }

        @Override
        public double evaluate(double[] values, int begin, int length)
                throws MathIllegalArgumentException {
            this.count = length;
            percentilesImpl.setData(values, begin, length);

            SimpleStats secondMoment = new SimpleStats();
            secondMoment.evaluate(values, begin, length);
            this.mean = secondMoment.getMean();
            this.min = secondMoment.getMin();
            this.max = secondMoment.getMax();

            this.stddev = new StandardDeviation(secondMoment).getResult();

            return Double.NaN;
        }

        @Override
        public CommonMetricsSnapshot copy() {
            CommonMetricsSnapshot result = new CommonMetricsSnapshot();
            result.count = count;
            result.min = min;
            result.max = max;
            result.mean = mean;
            result.stddev = stddev;
            result.percentilesImpl = percentilesImpl.copy();
            return result;
        }

        long getCount() {
            return count;
        }

        double getMin() {
            return min;
        }

        double getMax() {
            return max;
        }

        double getMean() {
            return mean;
        }

        double getStandardDeviation() {
            return stddev;
        }

        double getPercentile(double p) {
            return percentilesImpl.evaluate(p);
        }

        double[] getValues() {
            return percentilesImpl.getData();
        }
    }

    /**
     * Calculates min, max, mean (first moment), as well as the second moment in one go over
     * the value array.
     */
    private static class SimpleStats extends SecondMoment {
        private static final long serialVersionUID = 1L;

        private double min = Double.NaN;
        private double max = Double.NaN;

        @Override
        public void increment(double d) {
            if (d < min || Double.isNaN(min)) {
                min = d;
            }
            if (d > max || Double.isNaN(max)) {
                max = d;
            }
            super.increment(d);
        }

        @Override
        public SecondMoment copy() {
            SimpleStats result = new SimpleStats();
            SecondMoment.copy(this, result);
            result.min = min;
            result.max = max;
            return result;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public double getMean() {
            return m1;
        }
    }
}

