package cs3390.parser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	 
		String parts[] = value.toString().split("\t");
    	Text outputKey = new Text();
    	Text outputValue = new Text();
     	//key is k 
     	outputKey.set(parts[0].trim());
     	//val is everything else
     	outputValue.set(parts[1].trim());
     	context.write(outputKey, outputValue);
	 }
}