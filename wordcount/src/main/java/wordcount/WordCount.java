package wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount {
	final static String stopwords = "stopwords.txt";
        
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private String pattern= "^[a-z][a-z0-9']*$";
	    private Set<String> stopWords = new HashSet<String>();
	    
	    public void setup(Mapper.Context context){
	    	Configuration conf = context.getConfiguration();
	    	String filename = conf.get(stopwords);
	    	if(filename == null){
	    		return;
	    	}
	    	File thefile = new File(filename);
	    	BufferedReader fis;
	    	try{
	    		fis = new BufferedReader(new FileReader(thefile));
	    	}
	    	catch(FileNotFoundException e){
	    		e.printStackTrace();
	    		throw new RuntimeException("File not found exception ", e);
	    	}
	    	String stopword;
	    	try{
	    		while ((stopword = fis.readLine()) != null){
	    			stopWords.add(stopword);
	    		}
	    	}
	    	catch(IOException e){
	    		e.printStackTrace();
	    		throw new RuntimeException("Error reading word", e);
	    	}
	    }
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            String stringWord = word.toString().toLowerCase();
	            if (stringWord.matches(pattern)){
	            	if(stopWords.contains(stringWord)){
	            		continue;
	            	}
	                context.write(new Text(stringWord), one);
	            }
	            
	        }
	    }
	}
        
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        context.write(key, new IntWritable(sum));
	    }
	}
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "wordcount");
  
    job.setJarByClass(WordCount.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    //The step below will do partial aggregation
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}