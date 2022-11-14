package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.storage.BufferPool;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int min;
    private int max;
    private double width;
    private int numTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.numTuples = 0;
        this.width = Math.max((max - min + 1.0) / this.buckets.length, 1.0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if (v > max || v < min)
    	    throw new IllegalArgumentException("Illegal value to add");
        // Note that it's possible (v - min) / width exceeds boundary of buckets
    	int valueIndex = Math.min((int) ((v - min) / width), this.buckets.length - 1);
    	buckets[valueIndex]++;
    	numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int valueIndex = Math.min((int) ((v - min) / width), this.buckets.length - 1);
        switch (op) {
            case EQUALS:
                if (v > max || v < min)
                    return 0;
                int height = buckets[valueIndex];
                return 1.0 * height / width / numTuples;
            case GREATER_THAN:
                if (v < min)
                    return 1.0;
                if (v >= max)
                    return 0.0;
                double b_f = 1.0 * buckets[valueIndex] / numTuples;
                double b_part = ((valueIndex + 1) * width - v) / width;
                int cntOtherBuckets = 0;
                for (int i = valueIndex + 1; i < buckets.length; i++)
                    cntOtherBuckets += buckets[i];
                return b_f * b_part + 1.0 * cntOtherBuckets / numTuples;
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN:
                if (v <= min)
                    return 0.0;
                if (v > max)
                    return 1.0;
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v) - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN_OR_EQ:
                if (v <= min)
                    return 1.0;
                if (v > max)
                    return 0.0;
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN_OR_EQ:
                if (v >= max)
                    return 1.0;
                if (v < min)
                    return 0.0;
                return estimateSelectivity(Predicate.Op.EQUALS, v) + estimateSelectivity(Predicate.Op.LESS_THAN, v);
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        int cnt = 0;
        for (int bucket : buckets)
            cnt += bucket;
        return cnt * 1.0 / numTuples;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String res = "";
        res = res + "min: " + min + "max: " + max + "width: "+ width + "ntuples:" + numTuples;
        return res;
    }
}
