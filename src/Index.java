import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class Index {

    private final Controller controller;

    private final HashMap<String, Set<DstoreConnection>> index;
    private final HashMap<String, Integer> sizes;

    private final ConcurrentHashMap<String, CountDownLatch> storeAcks;
    private final ConcurrentHashMap<String, CountDownLatch> removeAcks;

    private final ReadWriteLock lock;

    public Index(Controller controller) {
        this.controller = controller;

        this.index = new HashMap<String, Set<DstoreConnection>>();
        this.sizes = new HashMap<String, Integer>();

        this.storeAcks = new ConcurrentHashMap<String, CountDownLatch>();
        this.removeAcks = new ConcurrentHashMap<String, CountDownLatch>();

        this.lock = new ReentrantReadWriteLock(true);
    }

    public boolean beginStore(String filename, Integer size) {
        this.lock.readLock().lock();
        if (this.index.containsKey(filename))
            return false;
        this.lock.readLock().unlock();

        this.lock.writeLock().lock();
        this.index.put(filename, null);
        this.sizes.put(filename, size);
        this.lock.writeLock().unlock();

        this.storeAcks.put(filename, new CountDownLatch(this.controller.getR()));

        return true;
    }

    public void endStore(String filename, Set<DstoreConnection> ds, boolean success) {
        this.lock.writeLock().lock();
        if (success)
            this.index.put(filename, ds);
        else {
            this.index.remove(filename);
            this.sizes.remove(filename);
        }
        this.lock.writeLock().unlock();
    }

    public List<DstoreConnection> getFileDstores(String filename) {
        List<DstoreConnection> ds = null;
        this.lock.readLock().lock();
        if (this.index.get(filename) != null)
            ds = new ArrayList<DstoreConnection>(this.index.get(filename));
        this.lock.readLock().unlock();
        return ds;
    }

    public Integer getFileSize(String filename) {
        return this.sizes.get(filename);
    }

    public void addStoreAck(String filename) {
        this.storeAcks.get(filename).countDown();
    }

    public void addRemoveAck(String filename) {
        this.removeAcks.get(filename).countDown();
    }

    public boolean awaitStore(String filename) {
        try {
            return this.storeAcks.get(filename).await(this.controller.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
    }
}
