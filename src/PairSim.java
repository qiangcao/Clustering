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
 * PairSim is to compute the pairwise similarity of user actions on each
 * shared ip address where both users have perform actions. 
 *
 * Output: "<uid1>,<uid2>,<sim>,<ip>,<isLazy>"
 */

public class PairSim extends Configured implements Tool {

 // use log4j for logging
 private static final Logger sLogger = Logger.getLogger(PairSim.class);
  
 // The time window size to determine if two events happen around
 // the same time
 public static int windowSize = 3600;

 // The threshold on the TV lenghth above which the TVs are consider long
 public static int tvLenBar = 5; 

 // number of reducers
 public static int reducers = 3000;

 public static class Reduce
    extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {

    public void configure(JobConf conf) {
      // load parameters
      windowSize = conf.getInt("ipps.windowSize", 3600);
      tvLenBar = conf.getInt("ipps.tvLenBar", 5);
    }               
   
    public void reduce(
      Text key,
      Iterator<Text> values,
      OutputCollector<NullWritable, Text> output,
      Reporter reporter
    ) throws IOException {

      // check the paremeters
      sLogger.info("windowSize: " + windowSize);
      sLogger.info("tvLenBar: " + tvLenBar);

      // Divide the users into two parts: activeUsers and lazyUsers
      Vector<UserActionProfile> activeUsers = new Vector<UserActionProfile>();
      Vector<UserActionProfile> lazyUsers = new Vector<UserActionProfile>();
      String ip = key.toString();
      int userCount = 0;

      while(values.hasNext()) {
        UserActionProfile curUser= new UserActionProfile(values.next().toString());
	if(curUser.getTimeVector().size() >= tvLenBar) // add to activeUsers
	  activeUsers.addElement(curUser);
	else lazyUsers.addElement(curUser);

        userCount++;
        // report progress
        if(userCount % 100 == 0)
          reporter.progress(); 
      }

      // compute similarity for lazy users
      boolean isLazy = true;
      compute_similarity(lazyUsers, ip, isLazy, output, reporter);
     
      // compute similarity for active users
      isLazy = false;
      compute_similarity(activeUsers, ip, isLazy, output, reporter);
    }

    // compute user similarity for a user set
    public void compute_similarity(Vector<UserActionProfile> userSet, String ip, 
	boolean isLazy, OutputCollector<NullWritable, Text> output, 
	Reporter reporter) throws IOException {
      int pairCount = 0;
      int numUsers = userSet.size();
      for(int i = 0; i < numUsers; i++) {
	UserActionProfile curUser = userSet.get(i);
	for(int j = i+1; j < numUsers; j++) {
	  UserActionProfile toComp = userSet.get(j);
	  double similarity = VectorUtils.vectorSimilarity(curUser.getTimeVector(),
		toComp.getTimeVector(), windowSize);
	  UserPair up = new UserPair(curUser.getID(), toComp.getID(), similarity,
		ip, isLazy); 
	  output.collect(NullWritable.get(), new Text(up.toString()));

 	  pairCount++;
	  if(pairCount % 100 == 0)
	    reporter.progress();
	}
      }
    }

 }
 
 public int run(String[] args) throws Exception {
   if (args.length < 2) {
     System.err.println(
         "Not enough arguments!\nRun as:\n"
         + "hadoop jar PairSim.jar PairSim [options] "
         + "INPUT_DIR OUTPUT_DIR\n\n"

         + "Available options are as below:\n"
         + "ipps.windowSize\tTime window size to determine if two events happen "
         + "around the same time\n"
         + "ipps.tvLenBar\tMinimum number of actions that a user should have "
         + "performed, in order to be enrolled into the active user set\n\n"

         + "Input format: uid\tip\ttimevector\n"
         + "timevector format: t1,t2,t3,...\n"
      );
     return 1;
   }
 
   Configuration configuration = getConf();
   JobConf conf = new JobConf(configuration, PairSim.class);
   conf.setJobName("ip_action_similarity");
       
   conf.setMapOutputKeyClass(Text.class);
   conf.setMapOutputValueClass(Text.class);
   conf.setOutputKeyClass(NullWritable.class);
   conf.setOutputValueClass(Text.class);
   conf.setMapperClass(PairSimMapper.class);
   conf.setReducerClass(Reduce.class);
   conf.setInputFormat(KeyValueTextInputFormat.class);
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
   int res = ToolRunner.run(new Configuration(), new PairSim(), args);
   System.exit(res);
 }
}
