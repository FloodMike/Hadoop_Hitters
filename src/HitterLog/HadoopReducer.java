/*
 * HadoopReducer.java
 *
 * Created on Apr 15, 2012, 5:30:20 PM
 */

package HitterLog;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This determines the average, maximimum and minimum for each category given to the reducer.
 * The function will aslo out put those values to a file.
 * @author mike
 */
public class HadoopReducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("HitterLog.HadoopReducer");

	@Override
   	public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
        // TODO code reducer logic here
        double	average	=	0.0,
        	max	=	0.0,
        	min	=	9999.0,
        	number	=	0.0;
        
        int	total	=	0;
        
        while (value.hasNext()) {

        	number	=	value.next().get();
        	average	+=	number;
        		
        	if(number > max){
        		max = number;
        	}
        	else if(number < min){
        		min = number;
        	}
        	
        	total++;
        	
        }
        
        average = average / (double) total;
        
        output.collect(key, null);
        output.collect(new Text("\tAverage"), new Text(Double.toString(average)));
        output.collect(new Text("\tMaximum"), new Text(Double.toString(max)));
        output.collect(new Text("\tMinimum"), new Text(Double.toString(min) + "\n"));
        
    }
}
