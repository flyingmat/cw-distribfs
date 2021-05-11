import java.util.*;
import java.io.*;
import java.net.*;

public abstract class Connection extends TCPClient implements Runnable {

    protected Connection(Socket ins, Socket outs) throws Exception {
        super(ins, outs);
    }

    protected abstract void processMessage(String msg);

    protected abstract void onDisconnect();

    @Override
    public void run() {
        try {
            String msg = "";
            do {
                msg = await();
                if (msg != null)
                    processMessage(msg);
            } while (msg != null);

            close();
        } catch (Exception e) {
            // when client disconnects ?
        }

        onDisconnect();
    }
}
