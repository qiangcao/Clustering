import java.util.*;
import java.io.*;
import java.lang.Math;

/*
 * VectorUtils provides utilities to for vector processing in IP temporal 
 * clustering. It contains method to convert time vectors to strings, to
 * compute hamming distance of vectors, etc. 
 *
 * we assume the elements in a vector are already sorted in an increasing 
 * order. This has been done in the hive-based data pipeline
 *
 */

public class VectorUtils {
  
  // The string format of a vector: "t1,t2,..."
  // Time points in the vector are seperated by commas
  // The hive-based data pipeline has already converted the vectors to
  // this format.
  public static Vector<Long> string2Vector(String str) {
    String[] parts = str.split(",");
    Vector<Long> v = new Vector<Long>();
    for(String part : parts) {
      v.addElement(Long.parseLong(part));
    }
    return v;
  }

  // Convert a vector to a string according to our string represenation
  public static String vector2String(Vector<Long> v) {
    StringBuilder result = new StringBuilder();
    char delim = ',';
    for(int i = 0; i < v.size(); i++) {
      result.append(v.get(i));
      if(i < v.size() - 1) {
        result.append(delim);
      }
    }
    return result.toString();
  }

  // Vector distance computation
  //
  // If two time points from vector v1 and v2 are within the same time
  // window, we call that they result in a common element for v1 and v2.
  // 
  // This function is to calculate the number of common elements in v1 
  // and v2. We normalize the nubmer of common elements by the length of
  // the shorter vector. The ratio is used as the similarity metric for
  // time vectors.
  public static double vectorSimilarity(Vector<Long> v1, Vector<Long> v2,
      double window) {
    int len1 = v1.size();
    int len2 = v2.size();
    assert len1 > 0 && len2 > 0: "Error: empty vector";
    int commons = 0; 
    int i = 0;
    int j = 0;
    while(i < len1 && j < len2) {
      long item1 = v1.get(i);
      long item2 = v2.get(j);
      if(Math.abs(item1 - item2) <= window/2.0) {  // item1 and item2 are in the same window
        commons += 1; // increment the number of common items
        i += 1; // the pointer of v1 moves one step forward
        j += 1; // the pointer of v2 moves one step forward
      }
      else if (item1 < item2){
        i += 1; // only move the pointer of the vector that has the smaller value
      }
      else j += 1;
    }

    // We define the similarity as ratio of the number of common items to
    // the min length of the two vectors.
    double similarity = (double) commons / Math.min((double) len1,  (double) len2);
    return similarity;
  }
}
