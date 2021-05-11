import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class Index {

    private final Controller controller;

    private final HashMap<String, List<DstoreConnection>> index;
    private final HashMap<String, Integer> sizes;

    private final ConcurrentHashMap<String, CountDownLatch> storeAcks;
    private final ConcurrentHashMap<String, CountDownLatch> removeAcks;

    private final ReadWriteLock lock;

    public Index(Controller controller) {
        this.controller = controller;

        this.index = new HashMap<String, List<DstoreConnection>>();
        this.sizes = new HashMap<String, Integer>();

        this.storeAcks = new ConcurrentHashMap<String, CountDownLatch>();
        this.removeAcks = new ConcurrentHashMap<String, CountDownLatch>();

        this.lock = new ReentrantReadWriteLock(true);
    }

    public boolean beginStore(String filename, Integer size) {
        try {
            this.lock.readLock().lock();
            if (this.index.containsKey(filename))
                return false;
        } finally {
            this.lock.readLock().unlock();
        }

        try {
            this.lock.writeLock().lock();
            this.index.put(filename, null);
            this.sizes.put(filename, size);
        } finally {
            this.lock.writeLock().unlock();
        }

        this.storeAcks.put(filename, new CountDownLatch(this.controller.getR()));

        return true;
    }

    public void endStore(String filename, List<DstoreConnection> ds, boolean success) {
        if (success) {
            try {
                this.lock.writeLock().lock();
                this.index.put(filename, ds);
            } finally {
                this.lock.writeLock().unlock();
                for (DstoreConnection d : ds)
                    d.store(filename);
            }
        } else {
            try {
                this.lock.writeLock().lock();
                this.index.remove(filename);
                this.sizes.remove(filename);
            } finally {
                this.lock.writeLock().unlock();
            }
        }
    }

    public List<DstoreConnection> beginRemove(String filename) {
        List<DstoreConnection> ds = null;
        try {
            this.lock.readLock().lock();
            ds = this.index.get(filename);
        } finally {
            this.lock.readLock().unlock();
        }

        if (ds != null) {
            try {
                this.lock.writeLock().lock();
                this.index.put(filename, null);
            } finally {
                this.lock.writeLock().unlock();
                for (DstoreConnection d : ds)
                    d.remove(filename);
            }
        }

        this.removeAcks.put(filename, new CountDownLatch(this.controller.getR()));

        return ds;
    }

    public void endRemove(String filename) {
        try {
            this.lock.writeLock().lock();
            this.index.remove(filename);
            this.sizes.remove(filename);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public List<DstoreConnection> getFileDstores(String filename) {
        List<DstoreConnection> ds = null;
        try {
            this.lock.readLock().lock();
            if (this.index.get(filename) != null)
                ds = new ArrayList<DstoreConnection>(this.index.get(filename));
            return ds;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public Integer getFileSize(String filename) {
        return this.sizes.get(filename);
    }

    public void addStoreAck(String filename) {
        if (this.storeAcks.containsKey(filename))
            this.storeAcks.get(filename).countDown();
    }

    public void addRemoveAck(String filename) {
        if (this.removeAcks.containsKey(filename))
            this.removeAcks.get(filename).countDown();
    }

    public boolean awaitStore(String filename) {
        try {
            return this.storeAcks.get(filename).await(this.controller.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean awaitRemove(String filename) {
        try {
            return this.removeAcks.get(filename).await(this.controller.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
    }

    public void removeDstore(DstoreConnection c) {
        try {
            this.lock.writeLock().lock();
            this.index.forEach((key, value) -> value.removeIf(t -> t.getPort().equals(c.getPort())));

            Set<String> files = new HashSet<String>(this.index.keySet());
            for (String f : files) {
                if (this.index.get(f).isEmpty()) {
                    this.index.remove(f);
                    this.sizes.remove(f);
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Set<String> fileList() {
        try {
            this.lock.readLock().lock();
            return this.index.keySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
