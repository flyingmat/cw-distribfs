import java.util.*;
import java.io.*;
import java.net.*;

public abstract class Connection extends TCPClient implements Runnable {

    protected boolean consume = true;

    protected Connection(Socket ins, Socket outs) throws Exception {
        super(ins, outs);
    }

    protected abstract void processMessage(String msg);

    protected void hold() {
        this.consume = false;
    }

    protected void resume() {
        this.consume = true;
    }

    @Override
    public void run() {
        try {
            String msg = "";
            do {
                if (this.consume && super.in.ready()) {
                    msg = await();
                    processMessage(msg);
                }
                Thread.sleep(100);
            } while (msg != null);

            close();
        } catch (Exception e) {
            // when client disconnects ?
            e.printStackTrace();
        }
    }
}
