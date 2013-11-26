/**
 * Table.java
 *
 * @autor Ivan Nikolchov
 * 
 */
package org.nikolchov.db;

import java.io.PrintWriter;
import java.util.ArrayList;

/**
 *
 * @author ivan
 */
public class Table extends ArrayList<ArrayList> {

  public Table() {
    super();
  }
  
  public Table(int size) {
    super(size);
  }
  
  public void print(PrintWriter out) {
    for (int i = 0; i < this.size(); i++) {
      printRow(out, i);
    }
  }

  public void printRow(PrintWriter out, int row_no) {
    for (Object o : this.get(0)) {
      out.println(o);
    }
  }
}
