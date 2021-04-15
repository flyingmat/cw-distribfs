import java.util.*;
import java.io.*;
import java.net.*;

public class Client {

    private Integer cport;
    private Integer timeout;

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    public Client(Integer cport, Integer timeout) throws Exception {
        this.cport = cport;
        this.timeout = timeout;

        this.socket = new Socket("127.0.0.1", this.cport);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream());
    }

    public String await() throws Exception {
        return this.in.readLine();
    }

    public void dispatch(String msg) {
        this.out.println(msg);
        this.out.flush();
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Error: invalid amount of command line arguments provided");
            return;
        }

        Integer cport;
        Integer timeout;
        try {
            cport = Integer.parseInt(args[0]);
            timeout = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Error: unable to parse command line arguments");
            return;
        }

        Client client;
        try {
            client = new Client(cport, timeout);
        } catch (Exception e) {
            System.out.println("Error: connection failed");
            return;
        }

        Scanner input = new Scanner(System.in);
        String line;
        while (!(line = input.nextLine()).equals("QUIT")) {
            client.dispatch(line);
            try {
                System.out.println("Received: " + client.await());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
