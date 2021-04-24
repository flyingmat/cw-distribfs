import java.util.*;
import java.util.stream.*;
import java.io.*;
import java.net.*;

public class Dstore extends TCPServer {

    private String file_folder;
    private TCPClient client;

    public DataConnection controller;
    public Map<String,Integer> files;

    public Dstore(Integer port, Integer cport, Integer timeout, String file_folder) throws Exception {
        super(port);
        this.file_folder = file_folder;
        this.client = new TCPClient("127.0.0.1", cport);

        init();

        new Thread(new Runnable() {
            public void run() {
                try {
                    Socket controller = socket.accept();
                    Dstore.this.controller = new DataConnection(Dstore.this, controller, Dstore.this.client.getSocketOut());
                    new Thread(Dstore.this.controller).start();
                    start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        this.client.dispatch("JOIN " + port);
    }

    @Override
    protected void onAccept(Socket socket) {
        try {
            new Thread(new DataConnection(this, socket, socket)).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() {
        this.files = new HashMap<String,Integer>();
        // testing
        files.put("file1.csv", 123);
        files.put("aaa.txt", 987);
        if (this.file_folder.equals("abab")) {
            files.put("loool.java", 3675);
        } else if (this.file_folder.equals("eee")) {
            files.put("xd.class", 111);
        }
    }

    public String list() {
        return "LIST BEGIN\n" +
            String.join(
                "\n",
                this.files.keySet().stream().map(s -> s + " " + this.files.get(s)).collect(Collectors.toList())
            ) + "\nLIST END";
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
