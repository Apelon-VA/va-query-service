package gov.vha.isaac.cradle.memory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.util.List;

/**
 * Created by kec on 4/8/15.
 */
public class MemoryManager {
    private static final Logger log = LogManager.getLogger();

    static class MyListener implements javax.management.NotificationListener {
        public void handleNotification(Notification notif, Object handback) {
            // handle notification
            log.info(notif);
        }
    }

    public static void startListener() {
        MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
        NotificationEmitter emitter = (NotificationEmitter) mbean;
        MyListener listener = new MyListener();
        emitter.addNotificationListener(listener, null, null);
    }



    public static void dumpMemoryInfo()
    {
        try
        {
            System.out.println( "\nDUMPING MEMORY INFO\n" );
            // Read MemoryMXBean
            MemoryMXBean memorymbean = ManagementFactory.getMemoryMXBean();
            System.out.println( "Heap Memory Usage: " + memorymbean.getHeapMemoryUsage() );
            System.out.println( "Non-Heap Memory Usage: " + memorymbean.getNonHeapMemoryUsage() );



            // Read Garbage Collection information
            List<GarbageCollectorMXBean> gcmbeans = ManagementFactory.getGarbageCollectorMXBeans();
            for( GarbageCollectorMXBean gcmbean : gcmbeans )
            {
                System.out.println( "\nName: " + gcmbean.getName() );
                System.out.println( "Collection count: " + gcmbean.getCollectionCount() );
                System.out.println( "Collection time: " + gcmbean.getCollectionTime() );
                System.out.println( "Memory Pools: " );
                String[] memoryPoolNames = gcmbean.getMemoryPoolNames();
                for( int i=0; i<memoryPoolNames.length; i++ )
                {
                    System.out.println( "\t" + memoryPoolNames[ i ] );
                }
            }

            // Read Memory Pool Information
            System.out.println( "Memory Pools Info" );
            List<MemoryPoolMXBean> mempoolsmbeans = ManagementFactory.getMemoryPoolMXBeans();
            for( MemoryPoolMXBean mempoolmbean : mempoolsmbeans )
            {
                System.out.println( "\nName: " + mempoolmbean.getName() );
                System.out.println( "Usage: " + mempoolmbean.getUsage() );
                System.out.println( "Collection Usage: " + mempoolmbean.getCollectionUsage() );
                System.out.println( "Peak Usage: " + mempoolmbean.getPeakUsage() );
                System.out.println( "Type: " + mempoolmbean.getType() );
                System.out.println( "Memory Manager Names: " ) ;
                String[] memManagerNames = mempoolmbean.getMemoryManagerNames();
                for( int i=0; i<memManagerNames.length; i++ )
                {
                    System.out.println( "\t" + memManagerNames[ i ] );
                }
                System.out.println( "\n" );
            }
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }
    }

}
