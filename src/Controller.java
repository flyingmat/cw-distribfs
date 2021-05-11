import java.util.*;
import java.util.concurrent.*;
import java.net.*;
import java.util.stream.Collectors;

public class Controller extends TCPServer {

    private final Object dstoreLock = new Object();

    private final Integer R;
    private final Integer timeout;

    private final Set<ClientConnection> clients;
    private final Set<DstoreConnection> dstores;

    private final Index index;

    public Controller(Integer cport, Integer R, Integer timeout, Integer rebalance_period) throws Exception {
        super(cport);

        this.R = R;
        this.timeout = timeout;

        this.clients = ConcurrentHashMap.newKeySet();
        this.dstores = ConcurrentHashMap.newKeySet();

        this.index = new Index(this);

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
        System.out.println("(i) New client detected");
        this.clients.add(c);
        new Thread(c).start();
    }

    public void removeClient(ClientConnection c) {
        System.out.println("(i) Client disconnected");
        this.clients.remove(c);
    }

    public void addDstore(DstoreConnection c) {
        synchronized(this.dstoreLock) {
            this.dstores.add(c);
            System.out.println("(i) New dstore detected - " + this.dstores.size());
        }

        new Thread(c).start();
    }

    public void removeDstore(DstoreConnection c) {
        synchronized(this.dstoreLock) {
            this.dstores.remove(c);
            System.out.println("(i) Dstore disconnected (" + c.getPort() + ")");
        }

        this.index.removeDstore(c);
    }

    public void store(ClientConnection c, String filename, Integer size) {

        List<DstoreConnection> ds;

        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            } else {
                ds = this.dstores.stream().sorted(
                    (d1, d2) -> Integer.compare(d1.getFileAmount(), d2.getFileAmount())
                ).limit(this.R).collect(Collectors.toList());
            }
        }

        if (!this.index.beginStore(filename, size)) {
            c.dispatch("ERROR_FILE_ALREADY_EXISTS");
            return;
        }

        String r = "STORE_TO";
        for (DstoreConnection d : ds)
            r += " " + d.getPort();

        c.dispatch(r);

        boolean complete = this.index.awaitStore(filename);

        if (complete) {
            c.dispatch("STORE_COMPLETE");
        }

        this.index.endStore(filename, ds, complete);
    }

    public void load(ClientConnection c, String filename, Integer i) {
        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }
        }

        List<DstoreConnection> ds = this.index.getFileDstores(filename);
        if (ds == null)
            c.dispatch("ERROR_FILE_DOES_NOT_EXIST");
        else if (i >= ds.size())
            c.dispatch("ERROR_LOAD");
        else
            c.dispatch("LOAD_FROM " + ds.get(i).getPort() + " " + this.index.getFileSize(filename));
    }

    public void remove(ClientConnection c, String filename) {
        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }
        }

        List<DstoreConnection> ds = this.index.beginRemove(filename);
        if (ds == null || (ds != null && ds.isEmpty())) {
            c.dispatch("ERROR_FILE_DOES_NOT_EXIST");
            return;
        }

        for (DstoreConnection d : ds) {
            d.dispatch("REMOVE " + filename);
        }

        boolean complete = this.index.awaitRemove(filename);
        c.dispatch("REMOVE_COMPLETE");
        System.out.println(" >> [CLIENT] REMOVE_COMPLETE");
        this.index.endRemove(filename);
    }

    public void list(ClientConnection c) {
        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }
        }

        c.dispatch("LIST " + String.join(" ", this.index.fileList()));
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
