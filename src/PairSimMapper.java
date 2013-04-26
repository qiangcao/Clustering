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

/*
 * Mapper for PairSim
 */

public class PairSimMapper
  extends MapReduceBase
  implements Mapper<Text, Text, Text, Text> {

  // The threshold on the TV lenghth above which the TVs are consider long
  public static int tvLenBar = 5;

  public void configure(JobConf conf) {
  // load the parameter
    tvLenBar = conf.getInt("ipps.tvLenBar", 5);
  }  

  public void map(
    Text key,
    Text value,
    OutputCollector<Text, Text> output,
    Reporter reporter
  ) throws IOException {

    String userid = key.toString(); // userid
    Scanner scanin = new Scanner(value.toString()); // ip + \t + time_array

    String ip = scanin.next();
    String timearr = scanin.next();
    String user_time = userid + "\t" + timearr;
    
    int arrLen = getTimeArrLen(timearr);
    String outkey;
    if(arrLen < tvLenBar) // a lazy user
      outkey = ip + "," + "T";
    else outkey = ip + "," + "F"; // an active user    

    output.collect(new Text(outkey), new Text(user_time));
    reporter.progress();
  }
  
  public int getTimeArrLen(String timearr){
    Vector<Long> timeVector = VectorUtils.string2Vector(timearr);
    return timeVector.size();
  }
}
