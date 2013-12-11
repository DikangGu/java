import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

public class TestConcurrentModificationException {

  @Test
  public void testModify() throws IOException, InterruptedException {
    Map<String, Integer> testMap = Collections.synchronizedMap(new HashMap<String, Integer>());
    for (int i = 0; i < 10; i++) {
      testMap.put(String.valueOf(i), i);
    }
    
    ItThread thread1 = new ItThread(testMap);
    PutThread thread2 = new PutThread(testMap);
    ItThread thread3 = new ItThread(testMap);
    
    //thread2.start();
    thread1.start();
    thread3.start();
    
    thread1.join();
    //thread2.join();
    thread3.join();
    
  }
  
  public class ItThread extends Thread {
    private Map<String, Integer> testMap;
    
    public ItThread(Map<String, Integer> testMap) {
      this.testMap = testMap;
    }
    
    @Override
    public void run() {
      Iterator<String> iter = testMap.keySet().iterator();
      while(iter.hasNext()) {
        String key = iter.next();
        if (testMap.get(key) % 2 == 0) {
          iter.remove();
        }
      }
    }
  }

  public class PutThread extends Thread {
    private Map<String, Integer> testMap;
    
    public PutThread(Map<String, Integer> testMap) {
      this.testMap = testMap;
    }
    
    @Override
    public void run() {
      for (int i = 10; i < 20; i++) {
        testMap.put(String.valueOf(i), i);
      }
      
      for (int i = 10; i < 20; i++) {
        testMap.remove(String.valueOf(i));
      }
    }
  }
   
}
