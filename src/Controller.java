import java.util.*;
import java.net.*;

public class Controller extends TCPServer {

    private Integer timeout;

    private ArrayList<ClientConnection> clients;
    private ArrayList<DstoreConnection> dstores;

    public Controller(Integer cport, Integer R, Integer timeout, Integer rebalance_period) throws Exception {
        super(cport);
        this.timeout = timeout;
        this.clients = new ArrayList<ClientConnection>();
        this.dstores = new ArrayList<DstoreConnection>();

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

    public void addDstore(DstoreConnection c) {
        System.out.println("(i) New dstore detected");
        this.dstores.add(c);
        new Thread(c).start();
    }

    public boolean store(String filename, Integer file_size) {
        for (DstoreConnection dstore : this.dstores) {
            ///dstore.dispatch("LIST");
            List<String> list = dstore.getList();
            for (String s : list)
                System.out.println(" list::" + s);
        }
        return true;
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
