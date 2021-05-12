import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.stream.Collectors;

public class Controller extends TCPServer {

    private final Integer R;
    private final Integer timeout;

    private final Set<ClientConnection> clients;
    private final Set<DstoreConnection> dstores;

    private final Index index;

    private final ReadWriteLock lock;

    public Controller(Integer cport, Integer R, Integer timeout, Integer rebalance_period) throws Exception {
        super(cport);

        this.R = R;
        this.timeout = timeout;

        this.clients = ConcurrentHashMap.newKeySet();
        this.dstores = ConcurrentHashMap.newKeySet();

        this.index = new Index(this);

        this.lock = new ReentrantReadWriteLock(true);

        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);

        new Thread(new Runnable() {
            public void run() {
                start();
            }
        }).start();
    }

    @Override
    protected void onAccept(Socket socket) {
        new Thread(new IdentifyConnection(this, socket)).start();
    }

    public void addClient(ClientConnection c) {
        this.clients.add(c);
        new Thread(c).start();
    }

    public void removeClient(ClientConnection c) {
        this.clients.remove(c);
    }

    public void addDstore(DstoreConnection c) {
        try {
            this.lock.writeLock().lock();
            this.dstores.add(c);
        } finally {
            this.lock.writeLock().unlock();
        }

        ControllerLogger.getInstance().dstoreJoined(c.getSocketIn(), c.getPort());
        new Thread(c).start();
    }

    public void removeDstore(DstoreConnection c) {
        try {
            this.lock.writeLock().lock();
            this.dstores.remove(c);
        } finally {
            this.lock.writeLock().unlock();
        }

        this.index.removeDstore(c);
    }

    public void store(ClientConnection c, String filename, Integer size) {

        List<DstoreConnection> ds;

        try {
            this.lock.readLock().lock();
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            } else {
                ds = this.dstores.stream().sorted(
                    (d1, d2) -> Integer.compare(d1.getFileAmount(), d2.getFileAmount())
                ).limit(this.R).collect(Collectors.toList());
            }
        } finally {
            this.lock.readLock().unlock();
        }

        if (!this.index.beginStore(filename, size)) {
            c.dispatch(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }

        String r = Protocol.STORE_TO_TOKEN;
        for (DstoreConnection d : ds)
            r += " " + d.getPort();

        c.dispatch(r);

        boolean complete = this.index.awaitStore(filename);

        if (complete) {
            c.dispatch(Protocol.STORE_COMPLETE_TOKEN);
        }

        this.index.endStore(filename, ds, complete);
    }

    public void load(ClientConnection c, String filename, Integer i) {
        try {
            this.lock.readLock().lock();
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
        } finally {
            this.lock.readLock().unlock();
        }

        List<DstoreConnection> ds = this.index.getFileDstores(filename);
        if (ds == null)
            c.dispatch(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        else if (i >= ds.size())
            c.dispatch(Protocol.ERROR_LOAD_TOKEN);
        else
            c.dispatch(Protocol.LOAD_FROM_TOKEN + " " + ds.get(i).getPort() + " " + this.index.getFileSize(filename));
    }

    public void remove(ClientConnection c, String filename) {
        try {
            this.lock.readLock().lock();
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
        } finally {
            this.lock.readLock().unlock();
        }

        List<DstoreConnection> ds = this.index.beginRemove(filename);
        if (ds == null || (ds != null && ds.isEmpty())) {
            c.dispatch(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        for (DstoreConnection d : ds) {
            d.dispatch(Protocol.REMOVE_TOKEN + " " + filename);
        }

        boolean complete = this.index.awaitRemove(filename);
        c.dispatch(Protocol.REMOVE_COMPLETE_TOKEN);
        this.index.endRemove(filename);
    }

    public void list(ClientConnection c) {
        try {
            this.lock.readLock().lock();
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
        } finally {
            this.lock.readLock().unlock();
        }

        c.dispatch(Protocol.LIST_TOKEN + " " + String.join(" ", this.index.fileList()));
    }

    public Integer getR() {
        return this.R;
    }

    public Integer getTimeout() {
        return this.timeout;
    }

    public Index getIndex() {
        return this.index;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Error: invalid amount of command line arguments provided");
            return;
        }

        Integer cport;
        Integer R;
        Integer timeout;
        Integer rebalance_period;
        try {
            cport = Integer.parseInt(args[0]);
            R = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            rebalance_period = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Error: unable to parse command line arguments");
            return;
        }

        Controller controller;
        try {
            controller = new Controller(cport, R, timeout, rebalance_period);
        } catch (Exception e) {
            System.out.println("Error: socket creation failed\n    (!) " + e.getMessage());
            return;
        }
    }
}
