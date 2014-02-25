/*
 * HadoopMapper.java
 *
 * Created on Apr 15, 2012, 5:09:18 PM
 */

package HitterLog;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author mike
 */
public class HadoopMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("HitterLog.HadoopMapper");

    // Defines a static constant for minimum plate appearances
    private final static int MINIMUM_PLATE_APPEARANCES    =    500;
    
    private DoubleWritable one = new DoubleWritable();
    private Text stat = new Text();
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
            throws IOException {
            	
        String line = value.toString();
        
        // Splits the data given in each line so that the program can gather the stats for each individual player
        String[] stats = line.split("\"");
        
        if(!stats[1].equalsIgnoreCase("Name")) {
            
            if(Integer.parseInt(stats[7]) >= MINIMUM_PLATE_APPEARANCES) {
                
                // parses the stats by their location within the string array
                stat.set("Games");
                one.set(Double.parseDouble(stats[5]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Plate Appearances");
                one.set(Double.parseDouble(stats[7]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Hits");
                one.set(Double.parseDouble(stats[9]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Home Runs");
                one.set(Double.parseDouble(stats[11]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Runs");
                one.set(Double.parseDouble(stats[13]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Runs Batted In");
                one.set(Double.parseDouble(stats[15]));
                output.collect(stat,one);
                stat.clear();
                
                String[] temp;
                
                stat.set("Walk Percentage");
                temp    =    stats[19].split("%");
                
                if(!temp[0].isEmpty())
                    one.set((Double.parseDouble(temp[0]) / 100.0));
                else
                    one.set(0.0);
                
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Strike Out Percentage");
                temp    =    stats[21].split("%");
                
                if(!temp[0].isEmpty())
                    one.set((Double.parseDouble(temp[0]) / 100.0));
                else
                    one.set(0.0);
                
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Isolated Power");
                one.set(Double.parseDouble(stats[23]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Batting Average On Balls In Play (BABIP)");
                one.set(Double.parseDouble(stats[25]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("Batting Average");
                one.set(Double.parseDouble(stats[27]));
                output.collect(stat, one);
                stat.clear();
                
                stat.set("On Base Percentage");
                one.set(Double.parseDouble(stats[29]));
                output.collect(stat,one);
                stat.clear();
                
                
                stat.set("Slugging Percentage");
                one.set(Double.parseDouble(stats[31]));
                output.collect(stat,one);
                stat.clear();
                
            }
        }
    }
}
