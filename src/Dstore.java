import java.util.*;
import java.io.*;
import java.net.*;

public class Dstore extends TCPServer {

    private TCPClient client;

    private List<String> files;

    public Dstore(Integer port, Integer cport, Integer timeout, String file_folder) throws Exception {
        super(port);
        this.client = new TCPClient("127.0.0.1", cport);

        init();

        new Thread(new Runnable() {
            public void run() {
                start();
            }
        }).start();
        this.client.dispatch("JOIN " + port);
    }

    @Override
    protected void onAccept(Socket socket) {
        try {
            new Thread(new DataConnection(this, socket, this.client.getSocketOut())).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() {
        this.files = new ArrayList<String>();
        // testing
        files.add("file1.csv");
        files.add("aaa.txt");
    }

    public String list() {
        return "LIST BEGIN\n" + String.join("\n", this.files) + "\nLIST END";
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Error: invalid amount of command line arguments provided");
            return;
        }

        Integer port;
        Integer cport;
        Integer timeout;
        try {
            port = Integer.parseInt(args[0]);
            cport = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("Error: unable to parse command line arguments");
            return;
        }

        Dstore dstore;
        try {
            dstore = new Dstore(port, cport, timeout, args[3]);
        } catch (Exception e) {
            System.out.println("Error: connection failed");
            return;
        }
    }
}
