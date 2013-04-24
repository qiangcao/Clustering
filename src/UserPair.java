import java.util.*;
import java.io.*;

/* 
 * User pair is the data structure designed to store the
 * similarity of two users on a particular ip address based
 * on their action history on that ip address.
 * We add the field isLazy to identify if this pair of users
 * are lazy (short history) or active. 
 */

public class UserPair {
  UserPairIDs userIDs;
  SimMetric similarity;
  
  // The string format of a user pair: 
  // "<uid1>,<uid2>,<sim>,<ip>,<isLazy>"
  public UserPair(String in) {
    String[] parts = in.split(",");
    this.userIDs = new UserPairIDs(parts[0], parts[1]);
    this.similarity = new SimMetric(Double.parseDouble(parts[2]),
			parts[3], parts[4].equals("T"));
  }
  
  public UserPair(String u1, String u2, double s, 
	String ip_addr, boolean b) {
    this.userIDs = new UserPairIDs(u1, u2);
    this.similarity = new SimMetric(s, ip_addr, b);
  }

  // Get the string representation of a user pair
  // all parts are separated by commas
  public String toString(){
    StringBuilder result = new StringBuilder();
    char delim = ',';
    result.append(this.userIDs.toString());
    result.append(delim);
    result.append(this.similarity.toString());
    return result.toString();
  }

  public UserPairIDs getUserIDs() {
    return this.userIDs;
  }

  public SimMetric getSimMetric() {
    return this.similarity;
  }
}
