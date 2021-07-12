package apriori;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.AprioriUtils;

import java.io.IOException;

/*
 * Reducer for all phases would collect the emitted itemId keys from all the mappers
 * and aggregate it to return the count for each itemId.
 */

public class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text itemSet, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {


        double minSup = Double.parseDouble(context.getConfiguration().get("minSup")); // Getting minsup from configuration
        int numTxns = context.getConfiguration().getInt("numTxns", 2); // Getting numTxns from configuration
        
        int sum = 0;
        for(IntWritable val : values) { // Sum up over all the values found in a given mapped key
        	sum += val.get();
        }
        if(!AprioriUtils.hasMinSupport(minSup, numTxns, sum)) { // If the presence of itemset less than necessary
        																// don't continue
        	return;
        }
        context.getCounter(AprioriDriver.File.LINES_WRITTEN).increment(1);
        IntWritable result = new IntWritable();
        result.set(sum);
        context.write(itemSet, result); // form the result
    }
}
