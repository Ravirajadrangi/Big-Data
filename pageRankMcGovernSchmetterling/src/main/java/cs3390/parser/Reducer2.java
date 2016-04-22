package cs3390.parser;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer2 extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		float sum = 0;
		Iterator<Text> it = values.iterator();
		while(it.hasNext()) {
			Float val = Float.parseFloat(it.next().toString());
			sum += val;
		}
		context.write(new Text() ,  new Text("V\t"  + key + "\t" + String.valueOf(sum)));
	}
}
