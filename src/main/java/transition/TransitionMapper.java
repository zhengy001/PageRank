package transition;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransitionMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		String[] fromTo = line.split("\t");
		
		if ( fromTo.length == 1 || fromTo[1].trim().equals("") ) {
			return;
		}
		
		String from = fromTo[0];
		String[] tos = fromTo[1].split(",");
		final int len = tos.length;
		for ( String to : tos ) {
			context.write(new Text(from), new Text(to + "=" + 1.0/len));
		}
	}
}
