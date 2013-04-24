import java.io.*;
import java.util.*;

public class SimMetric {
  double sim;
  String ip;
  boolean isLazy;
 
  public SimMetric(double d, String s, boolean b) {
    this.sim = d;
    this.ip = s;
    this.isLazy = b;
  }

  public SimMetric(String in) {
    String[] parts = in.split(",");
    this.sim = Double.parseDouble(parts[0]);
    this.ip = parts[1];
    this.isLazy = parts[2].equals("T");
  }

  public String toString() {
    StringBuilder res = new StringBuilder();
    char delim = ',';
    res.append(this.sim);
    res.append(delim);
    res.append(this.ip);
    res.append(delim);
    if(this.isLazy)
	res.append('T');
    else res.append('F');
    return res.toString();
  }

  public double getSim() {
    return this.sim;
  }

  public String getIP() {
    return this.ip;
  }

  public boolean getUserType() {
    return this.isLazy;
  }
} 
