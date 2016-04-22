package cs3390.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikipediaReducer extends Reducer<Text, Text, Text, Text> {
	private HashMap<String, String> pageLookup; 
    
	public void reduce(Text key, Iterable<Text> values, Context context)
    		throws IOException, InterruptedException {
       
    		String keyParts[] = key.toString().split("\t");
    		String title =  keyParts[0];
    		String id = keyParts[1];
    		for ( Iterator<Text> it = values.iterator(); it.hasNext();){
    			String val = it.next().toString();
    			String valueParts[] = val.split("\t");
    			float numLinks = Float.parseFloat(valueParts[0]);
    			if (pageLookup.containsKey(valueParts[1]) ) {
    				String outgoingLinkId = pageLookup.get(valueParts[1]); // lookup from multiple outputs
    				if (outgoingLinkId != null) {
    					context.write(new Text("M\t"+ id +"\t" + outgoingLinkId +"\t" + (1/numLinks) ),  new Text ());
    				}
    			}
    		}
    }
    
    public void setup(Context c) {
 		pageLookup = new HashMap<String, String>();     
    	
		// Get the cached archives/files
 		try {
 			Configuration conf = c.getConfiguration();
 			Path pt = new Path("/user/pageid");
 			FileSystem fs = FileSystem.get(conf);
 			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		
			String line;
			while ((line=br.readLine()) != null){
				String parts[] = line.split("\t");
				pageLookup.put(parts[0], parts[1]);
			}
			br.close();
		} 
		catch(IOException e) {
			e.printStackTrace();
		}
    }
}
