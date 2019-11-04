package transition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		context.write(new Text(line[0]), new Text(line[1]));
	}
}
