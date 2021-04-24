import java.util.*;
import java.io.*;
import java.net.*;

public class Dstore extends TCPServer {

    private final Object lock = new Object();

    private final String file_folder;
    private final TCPClient client;

    private DataConnection controller;
    private volatile List<String> files;

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
                    System.out.println("Error: unable to accept controller connection\n    (!) " + e.getMessage());
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
        this.files = new ArrayList<String>();
        File folder = new File(this.file_folder);
        if (!folder.isDirectory()) {
            folder.mkdirs();
        } else {
            for (File f : folder.listFiles())
                f.delete();
        }
    }

    public String list() { synchronized(this.lock) {
        return "LIST BEGIN\n" + String.join("\n", this.files) + "\nLIST END";
    }}

    public void store(String filename) { synchronized(this.lock) {
        this.files.add(filename);
    }}

    public String getFileFolder() {
        return this.file_folder;
    }

    public DataConnection getController() {
        return this.controller;
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
