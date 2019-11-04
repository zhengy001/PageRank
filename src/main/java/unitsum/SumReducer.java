package unitsum;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		
		for ( DoubleWritable value : values ) {
			sum += value.get();
		}
		
		DecimalFormat df = new DecimalFormat("#.0000");
		sum = Double.valueOf(df.format(sum));
		context.write(key, new DoubleWritable(sum));
	}
}
