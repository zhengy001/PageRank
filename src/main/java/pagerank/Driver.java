package pagerank;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import transition.RankMapper;
import transition.RankReducer;
import transition.TransitionMapper;
import unitsum.SumReducer;
import unitsum.UnitMapper;

public class Driver {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Options options = new Options();

		options.addOption(new Option("trans", true, "input transition file dir"));
        options.addOption(new Option("rank",  true, "input rank file dir"));
        options.addOption(new Option("unit",  true, "unit output dir"));
        options.addOption(new Option("times", true, "times of convergence"));

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
			cmd = parser.parse(options, args);
	        for ( int i = 0; i < Integer.parseInt(cmd.getOptionValue("times")); i++ ) {
	        	if ( buildTransition(cmd, i) == 1 ) {
	        		throw new RuntimeException("Error: Transition " + i + " Build Failed");
	        	}
	        	
	        	if ( sumUnit(cmd, i) == 1 ) {
	        		throw new RuntimeException("Error: Sum Unit " + i + " Failed");
	        	}
	        }
		} catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(Driver.class.getName(), options);

            System.exit(1);
		} catch (RuntimeException e) {
        	System.out.println(e.getMessage());
        	
        	System.exit(1);
		}
	}
	
	private static int buildTransition(CommandLine cmd, int n)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Transition"+n);
		
		job.setJarByClass(Driver.class);
		
		ChainMapper.addMapper(job, TransitionMapper.class, LongWritable.class,
				Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RankMapper.class, Text.class,
				Text.class, Text.class, Text.class, conf);
		
		job.setReducerClass(RankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(cmd.getOptionValue("trans")), TextInputFormat.class, TransitionMapper.class);
		MultipleInputs.addInputPath(job, new Path(cmd.getOptionValue("rank")+(n == 0 ? "" : n) ), TextInputFormat.class, RankMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("unit")+n));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private static int sumUnit(CommandLine cmd, int n) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("UnitSum"+n);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(UnitMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(cmd.getOptionValue("unit")+n));
		FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("rank")+(n+1)));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
