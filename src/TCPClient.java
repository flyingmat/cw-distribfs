import java.util.concurrent.*;
import java.io.*;
import java.net.*;

public class TCPClient {

    protected Socket ins;
    protected Socket outs;

    protected BufferedReader in;
    protected PrintWriter out;

    public TCPClient(String address, Integer port) throws Exception {
        this(new Socket(address, port));
    }

    public TCPClient(Socket socket) throws Exception {
        this(socket, socket);
    }

    public TCPClient(Socket ins, Socket outs) throws Exception {
        this.ins = ins;
        this.outs = outs;
        this.in = new BufferedReader(new InputStreamReader(ins.getInputStream()));
        this.out = new PrintWriter(outs.getOutputStream());
    }

    public void close() throws Exception {
        this.in.close();
        this.out.close();
    }

    public String await() throws Exception {
        return this.in.readLine();
    }

    public String await(Integer timeout) {
        Callable<String> task = new Callable<String>() {
            public String call() {
                try {
                    return await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<String> ack = executor.submit(task);
        try {
            String out = ack.get(timeout, TimeUnit.MILLISECONDS);
            System.out.println("* ACK received: " + out);
            return out;
        } catch (Exception e) {
            return null;
        }
    }

    public void dispatch(String msg) {
        this.out.println(msg);
        this.out.flush();
    }

    public Socket getSocketIn() {
        return this.ins;
    }

    public Socket getSocketOut() {
        return this.outs;
    }
}
