import java.io.*;
import java.net.*;

public abstract class Connection implements Runnable {

    protected Socket ins;
    protected Socket outs;

    private BufferedReader in;
    private PrintWriter out;

    protected Connection(Socket ins, Socket outs) throws Exception {
        this.ins = ins;
        this.outs = outs;
        this.in = new BufferedReader(new InputStreamReader(ins.getInputStream()));
        this.out = new PrintWriter(outs.getOutputStream());
    }

    protected String await() throws Exception {
        return this.in.readLine();
    }

    protected void dispatch(String msg) {
        this.out.println(msg);
        this.out.flush();
    }

    protected abstract void processMessage(String msg);
}
