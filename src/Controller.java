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
    private volatile Map<String, Integer> sizes;

    public Controller(Integer cport, Integer R, Integer timeout, Integer rebalance_period) throws Exception {
        super(cport);
        this.R = R;
        this.timeout = timeout;
        this.clients = new ArrayList<ClientConnection>();
        this.dstores = new ArrayList<DstoreConnection>();
        this.index = new HashMap<String, List<DstoreConnection>>();
        this.sizes = new HashMap<String, Integer>();

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

    public void removeClient(ClientConnection c) {
        synchronized(this.clientLock) {
            System.out.println("(i) Client disconnected");
            this.clients.removeIf(t -> t.getSocketIn().getLocalPort() == c.getSocketIn().getLocalPort());
        }
    }

    public void addDstore(DstoreConnection c) {
        synchronized(this.dstoreLock) {
            System.out.println("(i) New dstore detected");
            this.dstores.removeIf(t -> t.getPort().equals(c.getPort()));
            this.dstores.add(c);
        }
        synchronized(this.indexLock) {
            this.index.forEach((key, value) -> value.removeIf(t -> t.getPort().equals(c.getPort())));
            for (String key : this.index.keySet()) {
                if (this.index.get(key).isEmpty())
                    this.index.remove(key);
            }
            //this.index.forEach((key, value) -> System.out.println(key + " " + value.size()));
        }
        new Thread(c).start();
    }

    public void removeDstore(DstoreConnection c) {
        synchronized(this.dstoreLock) {
            System.out.println("(i) Dstore disconnected (" + c.getSocketIn().getLocalPort() + ")");
            this.clients.removeIf(t -> t.getSocketIn().getLocalPort() == c.getSocketIn().getLocalPort());
        }
    }

    public void store(ClientConnection c, String filename, Integer file_size) {

        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            } else {
                synchronized(this.indexLock) {
                    if (this.index.containsKey(filename)) {
                        // file already exists
                        c.dispatch("ERROR_FILE_ALREADY_EXISTS");
                        return;
                    } else {
                        this.index.put(filename, new ArrayList<DstoreConnection>());
                        this.sizes.put(filename, file_size);
                    }

                    Collections.shuffle(this.dstores);
                    List<DstoreConnection> ds = this.dstores.subList(0, this.R);

                    // take R lowest size lists and their dstores' ports
                    String r = "STORE_TO";
                    for (DstoreConnection dstore : ds)
                        r += " " + dstore.getPort();

                    for (DstoreConnection dstore : ds)
                        dstore.hold();

                    System.out.println(" >> [CLIENT] " + r);
                    c.dispatch(r);

                    boolean complete = true;
                    for (DstoreConnection dstore : ds) {
                        // each dstore has timeout or should be total ???
                        String ack = dstore.await(this.timeout);
                        System.out.println(" [DSTORE] :: " + ack);

                        if (ack == null || !ack.equals("STORE_ACK " + filename)) {
                            System.out.println("STORE_ACK not received ??");

                            complete = false;

                            break;
                        }
                    }

                    for (DstoreConnection dstore : ds) {
                        dstore.resume();
                    }

                    if (complete) {
                        for (DstoreConnection dstore : ds) {
                            this.index.get(filename).add(dstore);
                        }
                        c.dispatch("STORE_COMPLETE");
                    }
                    else {
                        this.index.remove(filename);
                        return;
                    }
                }
            }
        }
    }

    public void load(ClientConnection c, String filename, Integer i) {
        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            } else {
                synchronized(this.indexLock) {
                    List<DstoreConnection> dstores = this.index.get(filename);
                    if (dstores == null) {
                        // file does not exist
                        c.dispatch("ERROR_FILE_DOES_NOT_EXIST");
                    } else if (i >= dstores.size()) {
                        c.dispatch("ERROR_LOAD");
                    } else {
                        System.out.println(" >> [CLIENT] " + "LOAD_FROM " + dstores.get(i).getPort() + " " + this.sizes.get(filename));
                        c.dispatch("LOAD_FROM " + dstores.get(i).getPort() + " " + this.sizes.get(filename));
                    }
                }
            }
        }
    }

    public void remove(ClientConnection c, String filename) {
        synchronized(this.dstoreLock) {
            if (this.dstores.size() < this.R) {
                // log, not enough dstores, STOP HERE
                c.dispatch("ERROR_NOT_ENOUGH_DSTORES");
                return;
            } else {
                synchronized(this.indexLock) {
                    List<DstoreConnection> dstores = this.index.get(filename);
                    if (dstores == null) {
                        // file does not exist
                        c.dispatch("ERROR_FILE_DOES_NOT_EXIST");
                    } else {
                        for (DstoreConnection dstore : dstores) {
                            dstore.hold();

                            System.out.println(" >> [DSTORE] REMOVE " + filename);
                            dstore.dispatch("REMOVE " + filename);
                            String ack = dstore.await(this.timeout);

                            if (ack != null) {
                                if (ack.equals("REMOVE_ACK " + filename))
                                    System.out.println(" [DSTORE] :: " + ack);
                                else {
                                    String[] ws = ack.split(" ");
                                    if (ws.length == 2 && ws[0].equals("ERROR_FILE_DOES_NOT_EXIST"))
                                        System.out.println("(!) " + ack);
                                    else
                                        System.out.println("(!) REMOVE_ACK malformed (" + ack + ")");
                                }
                            } else {
                                // timeout reached
                            }

                            dstore.resume();
                        }

                        this.index.remove(filename);

                        c.dispatch("REMOVE_COMPLETE");
                    }
                }
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
