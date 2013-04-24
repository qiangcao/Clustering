import java.util.*;
import java.io.*;

/* 
 * User action profile is the data structure designed to 
 * store the user history on a particular ip address. It
 * includes the user id and the vector of action time on
 * the ip address
 */

public class UserActionProfile {
  String userid;
  Vector<Long> timeVector;
  
  public UserActionProfile(String in) {
    Scanner scanin = new Scanner(in);
    this.userid = scanin.next();
    String timearr = scanin.next();
    this.timeVector = VectorUtils.string2Vector(timearr);
  }
  
  public UserActionProfile(String id, Vector<Long> v) {
    this.userid = id;
    this.timeVector = v;
  }

  // Get the string representation of the user action profile
  // the userid and the time vector (string) is separated by
  // a Tab
  public String toString(){
    String timearr = VectorUtils.vector2String(this.timeVector);
    String out = this.userid + "\t" + timearr;
    return out;
  }

  public String getID() {
    return this.userid;
  }

  public Vector<Long> getTimeVector() {
    return this.timeVector;
  }

  public String getTimeVectorByString() {
    return VectorUtils.vector2String(this.timeVector);
  }

}
