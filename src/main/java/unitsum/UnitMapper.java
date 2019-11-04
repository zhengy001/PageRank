package unitsum;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnitMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] ranks = value.toString().split("\t");
		
		context.write(new Text(ranks[0]), new DoubleWritable(Double.parseDouble(ranks[1])));
	}
}
