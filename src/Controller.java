import java.util.*;
import java.lang.Math.*;
import java.net.*;

public class Controller extends TCPServer {

    private Integer R;
    private Integer timeout;

    private ArrayList<ClientConnection> clients;
    private ArrayList<DstoreConnection> dstores;

    public Controller(Integer cport, Integer R, Integer timeout, Integer rebalance_period) throws Exception {
        super(cport);
        this.R = R;
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

    private Integer storageSize(List<String> list) {
        return list.stream().map(s -> {
            try {
                return Integer.parseInt(s.split(" ")[1]);
            } catch (NumberFormatException e) {
                return 0;
            }
        }).reduce(0, Integer::sum);
    }

    public boolean store(ClientConnection c, String filename, Integer file_size) {
        List<List<String>> lists = new ArrayList<List<String>>();
        Map<List<String>, DstoreConnection> ld = new HashMap<>();

        // get dstores' file lists
        for (DstoreConnection dstore : this.dstores) {
            List<String> list = dstore.getList();
            if (list != null) {
                lists.add(list);
                ld.put(list, dstore);

                for (String s : list)
                    System.out.println(" list::" + s);
            } else {
                // no ack, no list returned, idk
            }
        }

        if (lists.size() < this.R) {
            // log, not enough dstores, STOP HERE
            System.out.println("(!) Less than R dstores connected");
            return false;
        }

        // sort the file lists, lower total size first
        Collections.sort(
            lists,
            (l1, l2) -> Integer.compare(storageSize(l1), storageSize(l2))
        );

        for (List<String> l : lists) {
            System.out.println(storageSize(l) + " " + ld.get(l).getPort());
        }

        List<List<String>> rs = new ArrayList<>();

        // take R lowest size lists and their dstores' ports
        String r = "STORE_TO";
        for (int i = 0; i < this.R; i++) {
            r += " " + ld.get(lists.get(i)).getPort();
            rs.add(lists.get(i));
        }

        for (List<String> l : rs) {
            ld.get(l).hold();
        }

        System.out.println(r);
        c.dispatch(r);

        for (List<String> l : rs) {
            // each dstore has timeout or should be total ???
            String ack = ld.get(l).await(this.timeout);
            if (ack == null || !ack.equals("STORE_ACK " + filename)) {
                // log
            }
            ld.get(l).resume();
        }

        c.dispatch("STORE_COMPLETE");
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
