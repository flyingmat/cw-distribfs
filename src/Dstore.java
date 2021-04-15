import java.util.*;
import java.io.*;
import java.net.*;

public class Dstore {

    private Integer port;
    private Integer cport;
    private Integer timeout;
    private String file_folder;

    private ServerSocket ins;
    private Socket outs;
    private BufferedReader in;
    private PrintWriter out;

    public Dstore(Integer port, Integer cport, Integer timeout, String file_folder) throws Exception {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;

        this.ins = new ServerSocket(this.port);
        this.outs = new Socket("127.0.0.1", this.cport);
        this.out = new PrintWriter(outs.getOutputStream());

        start();
    }

    public String await() throws Exception {
        return this.in.readLine();
    }

    public void dispatch(String msg) {
        this.out.println(msg);
        this.out.flush();
    }

    private void start() {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        ins.accept();
                    } catch (Exception e) {
                        System.out.println("Error: unable to accept controller connection\n    (!) " + e.getMessage());
                    }
                }
            }
        }).start();

        dispatch("JOIN " + port);
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
