package cs3390.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

// Authors: Neal McGovern and David Schmetterling

public class PageRank {
	
	public static void main(String[] args) throws Exception {
		
		// path to matrix
		// path to vector
		// temp output 
		// final output
		Path m = new Path(args[0]);
		Path v = new Path(args[1]);

		for (int i = 0; i < 20; i++ ) {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Job job = new Job(conf, "Mapper1");
			job.setJarByClass(PageRank.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(Mapper1.class); 
			job.setReducerClass(Reducer1.class);
	        job.setInputFormatClass(TextInputFormat.class);
	    	job.setOutputFormatClass(TextOutputFormat.class);
	    	FileInputFormat.addInputPath(job, m);
	    	FileInputFormat.addInputPath(job, v);
	    	fs.delete(new Path(args[2]));
	    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    	boolean success = job.waitForCompletion(true);
	    	
	    	if (success ) {
		    	Job job2 = new Job(conf, "Mapper1");
				job2.setJarByClass(PageRank.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				job2.setMapperClass(Mapper2.class); 
				job2.setReducerClass(Reducer2.class);
		        job2.setInputFormatClass(TextInputFormat.class);
		    	job2.setOutputFormatClass(TextOutputFormat.class);
		    	FileInputFormat.addInputPath(job2, new Path(args[2]));
		    	fs.delete(new Path(args[3]));
		    	FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		    	v = new Path(args[3]);
		    	job2.waitForCompletion(true);
	    	}
	    }
	}
}
