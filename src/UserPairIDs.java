import java.io.*;
import java.util.*;

public class UserPairIDs {
  String uid1;
  String uid2;
  
  public UserPairIDs(String s1, String s2) {
    this.uid1 = s1;
    this.uid2 = s2;
  }
 
  public UserPairIDs(String in) {
    String[] parts = in.split(",");
    this.uid1 = parts[0];
    this.uid2 = parts[1];
  }
 
  public String toString() {
    StringBuilder res = new StringBuilder();
    char delim = ',';
    res.append(this.uid1);
    res.append(delim);
    res.append(this.uid2);
    return res.toString();
  }
  
  public String getUID1() {
    return this.uid1;
  }
  
  public String getUID2() {
    return this.uid2;
  }
}
