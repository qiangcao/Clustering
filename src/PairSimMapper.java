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
  extends org.apache.hadoop.mapred.MapReduceBase
  implements Mapper<Text, Text, Text, Text> {

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
    output.collect(new Text(ip), new Text(user_time));
    reporter.progress();
  }
}
