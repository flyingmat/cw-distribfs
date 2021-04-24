import java.util.*;
import java.net.*;

public class Controller extends TCPServer {

    private final Object dstoreLock = new Object();
    private final Object clientLock = new Object();
    private final Object storeLock = new Object();
    private final Object indexLock = new Object();

    private final Integer R;
    private final Integer timeout;

    private volatile List<ClientConnection> clients;
    private volatile List<DstoreConnection> dstores;
    private volatile Map<String, List<DstoreConnection>> index;

    public Controller(Integer cport, Integer R, Integer timeout, Integer rebalance_period) throws Exception {
        super(cport);
        this.R = R;
        this.timeout = timeout;
        this.clients = new ArrayList<ClientConnection>();
        this.dstores = new ArrayList<DstoreConnection>();
        this.index = new HashMap<String, List<DstoreConnection>>();

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
        synchronized(this.clientLock) {
            System.out.println("(i) New client detected");
            this.clients.add(c);
        }
        new Thread(c).start();
    }

    public void addDstore(DstoreConnection c) {
        synchronized(this.dstoreLock) {
            System.out.println("(i) New dstore detected");
            this.dstores.add(c);
        }
        new Thread(c).start();
    }

    public void store(ClientConnection c, String filename, Integer file_size) {
        synchronized(this.indexLock) {
            if (this.index.containsKey(filename)) {
                // file already exists
                c.dispatch("ERROR_FILE_ALREADY_EXISTS");
                return;
            } else {
                this.index.put(filename, new ArrayList<DstoreConnection>());
            }
        }

        Map<DstoreConnection, Integer> lists = new HashMap<>();

        // get dstores' file lists
        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                System.out.println("(!) Less than R dstores connected");
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            }

            for (DstoreConnection dstore : this.dstores) {
                List<String> list = dstore.getList();
                if (list != null) {
                    lists.put(dstore, list.size());
                    System.out.println(Arrays.toString(list.toArray()));
                } else {
                    // no ack, no list returned, idk
                }
            }
        }

        if (lists.size() < this.R) {
            // log, not enough lists received, continue
        }

        List<DstoreConnection> ds = new ArrayList<>(lists.keySet());
        Collections.sort(ds, (d1, d2) -> Integer.compare(lists.get(d1), lists.get(d2)));
        ds.subList(this.R, ds.size()).clear();

        // take R lowest size lists and their dstores' ports
        String r = "STORE_TO";
        for (int i = 0; i < this.R; i++) {
            r += " " + ds.get(i).getPort();
        }

        synchronized(this.storeLock) {
            for (DstoreConnection dstore : ds) {
                dstore.hold();
            }

            System.out.println(r);
            c.dispatch(r);

            for (DstoreConnection dstore : ds) {
                // each dstore has timeout or should be total ???
                String ack = dstore.await(this.timeout);
                if (ack == null || !ack.equals("STORE_ACK " + filename)) {
                    // log
                }
                dstore.resume();
            }

            c.dispatch("STORE_COMPLETE");
        }

        synchronized(this.indexLock) {
            for (DstoreConnection dstore : ds) {
                this.index.get(filename).add(dstore);
            }
        }
    }

    public Integer getTimeout() {
        return this.timeout;
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
