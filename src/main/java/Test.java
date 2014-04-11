/**
 * Created by david on 10/04/14.
 */

import com.elastacloud.spark.logger.AzureBlobStorageLogger;
import org.apache.log4j.*;

public class Test {
    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure((new Object() {}).getClass().getClassLoader().getResource("log4j.properties"));
        System.setProperty("log4j.debug", "");
        //AzureBlobStorageLogger loggerAppender = new AzureBlobStorageLogger();
      //  Logger root = Logger.getRootLo;

       // root.addAppender(loggerAppender);
       // root.setLevel(Level.INFO);

       Logger logger = Logger.getLogger("Test");

        for(int i = 0; i < 1; i++)
            logger.log(Level.INFO, i +" Perhaps you have wondered how predictable machines like computers can generate randomness. In reality, most random numbers used in computer programs are pseudo-random, which means they are generated in a predictable fashion using a mathematical formula. This is fine for many purposes, but it may not be random in the way you expect if you're used to dice rolls and lottery drawings.");



    }
}
