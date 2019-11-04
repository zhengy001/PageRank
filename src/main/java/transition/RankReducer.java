package transition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> transitionUnit = new ArrayList<String>();
		double factor = 0;
		
		for(Text value: values) {
			if ( value.toString().contains("=") ) {
				transitionUnit.add(value.toString());
			} else {
				factor = Double.parseDouble(value.toString());
			}
		}
		
		for ( String unit: transitionUnit ) {
			String[] elements = unit.split("=");
			
			double relation = Double.parseDouble(elements[1]);
			String outputValue = String.valueOf(relation*factor);
			
			context.write(new Text(elements[0]), new Text(outputValue));
		}
	}
}
