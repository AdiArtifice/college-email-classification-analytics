package com.bda.email;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * DomainMonthCountReducer
 *
 * Sums the per-key counts emitted by DomainMonthCountMapper.
 *
 * This class is used both as the job's Reducer AND its Combiner (see
 * DomainMonthCountDriver). Summation is associative and commutative, so
 * running it as a Combiner on each Mapper's local output is safe and
 * significantly reduces shuffle I/O across the cluster -- a standard
 * MapReduce optimisation worth calling out in the project viva.
 */
public class DomainMonthCountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
