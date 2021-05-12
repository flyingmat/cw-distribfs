import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;

public class Dstore extends TCPServer {

    private final Integer port;
    private final String file_folder;

    private final TCPClient client;
    private DataConnection controller;

    private final Set<String> files;

    public Dstore(Integer port, Integer cport, Integer timeout, String file_folder) throws Exception {
        super(port);
        this.port = port;
        this.file_folder = file_folder;

        this.client = new TCPClient("127.0.0.1", cport);

        this.files = ConcurrentHashMap.newKeySet();

        init();
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, this.port);

        new Thread(new Runnable() {
            public void run() {
                try {
                    Socket controller = socket.accept();
                    Dstore.this.controller = new DataConnection(Dstore.this, controller, Dstore.this.client.getSocketOut());
                    new Thread(Dstore.this.controller).start();
                    start();
                } catch (Exception e) {
                    System.out.println("Error: unable to accept connection\n    (!) " + e.getMessage());
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
        File folder = new File(this.file_folder);
        if (!folder.isDirectory()) {
            folder.mkdirs();
        } else {
            for (File f : folder.listFiles())
                f.delete();
        }
    }

    public String list() {
        return "";//"LIST BEGIN\n" + String.join("\n", this.files) + "\nLIST END";
    }

    public void store(String filename) {
        this.files.add(filename);
    }

    public Integer getPort() {
        return this.port;
    }

    public String getFileFolder() {
        return this.file_folder;
    }

    public DataConnection getController() {
        return this.controller;
    }

    public Integer getFileAmount() {
        return this.files.size();
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
