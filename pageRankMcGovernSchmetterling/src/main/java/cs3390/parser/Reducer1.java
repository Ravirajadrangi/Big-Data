package cs3390.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.imageio.IIOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		ArrayList<String> mList = new ArrayList<String>();
		String parts[];
		String rowID = "";
		float val = 0, vVal = 0;
		
		while(it.hasNext()){
			parts = it.next().toString().split("\t");
			vVal = 0;
			if(parts[0].contains("M"))    // if matrix 
				mList.add(parts[1] + "," + parts[2]);
			else
				vVal = Float.parseFloat(parts[1]);
		}
	
		for(int i = 0; i < mList.size(); i++){
			parts = mList.get(i).split(",");
			rowID = parts[0].trim(); 
			val = Float.parseFloat(parts[1]);
			context.write(new Text(rowID), new Text(String.valueOf(val*vVal)));
		}
	}
}
