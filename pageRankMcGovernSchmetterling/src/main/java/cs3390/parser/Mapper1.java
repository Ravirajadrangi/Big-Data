package cs3390.parser;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	//M, pageid(row), outpageid(col), value
    	
    	Text outputKey = new Text();
    	Text outputValue = new Text();
    	
    	String line = value.toString();
        
        ArrayList<String> inList = new ArrayList<String>(Arrays.asList(line.split("\t")));
        int k, i;
        
        if(inList.get(0).contains("M")) {
        	
        	outputKey.set(new Text(inList.get(2).trim()));
        	outputValue.set(new Text("M" + "\t" + inList.get(1).trim() + "\t" + inList.get(3).trim()));

        	context.write(outputKey, outputValue);
        }
        else {
			outputKey.set(new Text(inList.get(1).trim()));
			outputValue.set("V\t" + inList.get(2).trim());
			context.write(outputKey, outputValue);
        }
    }
	
}
