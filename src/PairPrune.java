import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.log4j.Logger;

/*
 * PairPrune is to screen out the pairs of users who are similar enough
 * to be considered as being loosely synchronized. 
 *
 * There are two cases in which users are similar, depending
 * on the length of time vectors on ip addresses.
 * 1. There exists at least one IP-TV pair in each list that is very 
 * similar and time vectors (TVs) are long. This means that the users
 * manifest lockstep behavior from at least one ip address, and the 
 * evidence is strong due to the long action history.
 * 2. There exist multiple IP-TV pairs and each pair is very similar, 
 * but the time vectors (TVs) are short. This means we require 
 * multiple-ip matches to decide on short time vectors. A single short 
 * time vectors may introduce noise, but if similar short time vectors
 * consistently happen on many ip addresses, this is likely to be a 
 * synchronized attack or campaign over the Internet. This type of attack
 * is more advanced.
 */

public class PairPrune extends Configured implements Tool {

 // use log4j for logging
 private static final Logger sLogger = Logger.getLogger(PairPrune.class);
 
 // the minimum similarity of a TV pair from the same IP address for long TVs
 public static double tvSimBarL = 0.7;

 // the minimum simiarity of short TV pairs
 public static double tvSimBarS = 0.6;

 // the minimum IP addresses that users should match on short TVs
 public static int ipNumBar = 4;

 // number of reducers
 public static int reducers = 3000;


 public static class Map
   extends org.apache.hadoop.mapred.MapReduceBase
   implements Mapper<LongWritable, Text, Text, Text> {

      // map users to reducers based on user ids of each pair
      public void map(
        LongWritable key,
        Text value,
        OutputCollector<Text, Text> output,
        Reporter reporter
      ) throws IOException {

        String line = value.toString();
	UserPair up = new UserPair(line);

        output.collect(new Text(up.getUserIDs().toString()), new Text(up.getSimMetric().toString()));

        reporter.incrCounter("IPPP Stats", "Num Entries", 1);
        reporter.progress();
      }
 }

 public static class Reduce
    extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {

    public void configure(JobConf conf) {
      // load parameters
      tvSimBarL = conf.getFloat("ippp.tvSimBarL", 0.7f);
      tvSimBarS = conf.getFloat("ippp.tvSimBarS", 0.6f);
      ipNumBar = conf.getInt("ippp.ipNumBar", 4);
    }               

    // in the reduce phase we cluster users from the same ip address (subnet)   
    public void reduce(
      Text key,
      Iterator<Text> values,
      OutputCollector<NullWritable, Text> output,
      Reporter reporter
    ) throws IOException {

      // check the paremeters
      sLogger.info("tvSimBarL: " + tvSimBarL);
      sLogger.info("tvSimBarS: " + tvSimBarS);
      sLogger.info("ipNumBar: " + ipNumBar);

      UserPairIDs upis = new UserPairIDs(key.toString());
      boolean iSimL = false; // similar active users with long TVs
      int numSimS = 0; // number of similar ips that lazy users with short TVs have     
      
      while(values.hasNext()) {
        SimMetric curSim= new SimMetric(values.next().toString());
	if(curSim.getSim() >= tvSimBarL && curSim.getUserType() == false) 
	  iSimL = true;
        if(curSim.getSim() >= tvSimBarS && curSim.getUserType() == true)
          numSimS++;
      }
      String delim = new String("\t");
      if( (iSimL == true) && (numSimS >= ipNumBar) ) {
 	// have both at least one similar long TV and enough short TVs
	String outStr = upis.getUID1() + delim + upis.getUID2() + delim + "B";
        output.collect(NullWritable.get(), new Text(outStr));
	// Generate the symmetric pair
	String outStrSym = upis.getUID2() + delim + upis.getUID1() + delim + "B";
        output.collect(NullWritable.get(), new Text(outStr));
      }
      else if (iSimL == true) { // only have similar long TVs
        String outStr = upis.getUID1() + delim + upis.getUID2() + delim + "L";
        output.collect(NullWritable.get(), new Text(outStr));
        // Generate the symmetric pair
        String outStrSym = upis.getUID2() + delim + upis.getUID1() + delim + "L";
        output.collect(NullWritable.get(), new Text(outStr));
      }
      else if (numSimS >= ipNumBar) { // only have many similar short TVs
        String outStr = upis.getUID1() + delim + upis.getUID2() + delim + "S";
        output.collect(NullWritable.get(), new Text(outStr));
        // Generate the symmetric pair
        String outStrSym = upis.getUID2() + delim + upis.getUID1() + delim + "S";
        output.collect(NullWritable.get(), new Text(outStr));
      }
      else;
    }   
 }

  
 public int run(String[] args) throws Exception {
   if (args.length < 2) {
     System.err.println(
         "Not enough arguments!\nRun as:\n"
         + "hadoop jar PairPrune.jar PairPrune [options] "
         + "INPUT_DIR OUTPUT_DIR\n\n"

         + "Available options are as below:\n"
         + "ippp.tvSimBarL\tthe minimum similarity of a TV pair from the same "
	 + "IP address for long TVs\n"
         + "ippp.tvSimBarS\tthe minimum simiarity of short TV pairs\n"
	 + "ippp.ipNumBar\tthe minimum IP addresses that users should match on "
         + "short TVs\n\n"	
     );
     return 1;
   }
 
   Configuration configuration = getConf();
   JobConf conf = new JobConf(configuration, PairPrune.class);
   conf.setJobName("ip_action_sim_prum");
       
   conf.setMapOutputKeyClass(Text.class);
   conf.setMapOutputValueClass(Text.class);
   conf.setOutputKeyClass(NullWritable.class);
   conf.setOutputValueClass(Text.class);
   conf.setMapperClass(Map.class);
   conf.setReducerClass(Reduce.class);
   conf.setInputFormat(TextInputFormat.class);
   conf.setOutputFormat(TextOutputFormat.class);
   conf.setNumReduceTasks(reducers);

   // set the input directory
   FileInputFormat.setInputPaths(conf, new Path(args[0]));

   // clean the output directory
   FileSystem fs = FileSystem.get(conf);
   try {
     fs.delete(new Path(args[1]),true);     
   } catch (IOException e) {
     System.err.println(e);
   }
   
   // set the output directory
   FileOutputFormat.setOutputPath(conf, new Path(args[1]));
   JobClient.runJob(conf);
   return 0;
 }

 public static void main(String[] args) throws Exception {
   // Let ToolRunner handle generic command-line options
   int res = ToolRunner.run(new Configuration(), new PairPrune(), args);
   System.exit(res);
 }
}
