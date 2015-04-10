package gov.vha.isaac.cradle.memory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by kec on 4/10/15.
 */
public class WriteToDiskCache {

    private static final int WRITE_INTERVAL_IN_SECONDS = 15;

    static Thread writerThread;

    static ConcurrentSkipListSet<MemoryManagedReference> cacheSet = new ConcurrentSkipListSet<>();

    static {
        writerThread = new Thread(new WriteToDiskRunnable(), "WriteToDiskCache thread");
        writerThread.setDaemon(true);
    }

    public static class WriteToDiskRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                Optional<MemoryManagedReference> reference = cacheSet.stream().filter(memoryManagedReference -> {
                    if (memoryManagedReference.get() == null) {
                        cacheSet.remove(memoryManagedReference);
                        return false;
                    }
                    return false;
                }).max((o1, o2) -> {
                    return o1.timeSinceLastUnwrittenUpdate().compareTo(o2.timeSinceLastUnwrittenUpdate());
                });
                boolean written = false;
                if (reference.isPresent()) {
                    written = true;
                    MemoryManagedReference ref = (MemoryManagedReference) reference.get();
                    if (ref.timeSinceLastUnwrittenUpdate().compareTo(Duration.of(WRITE_INTERVAL_IN_SECONDS, ChronoUnit.SECONDS)) > 0) {
                        ref.write();
                    }
                }
                if (!written) {
                    try {
                        writerThread.wait(WRITE_INTERVAL_IN_SECONDS * 1000);
                    } catch (InterruptedException e) {
                        // continue work
                    }
                }
            }
        }
    }


    public static void addToCache(MemoryManagedReference newRef) {
        cacheSet.add(newRef);
    }
}
