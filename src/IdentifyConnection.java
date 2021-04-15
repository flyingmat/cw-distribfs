import java.io.*;
import java.net.*;

public class IdentifyConnection implements Runnable {

    private Controller controller;
    private Socket socket;

    public IdentifyConnection(Controller controller, Socket socket) {
        this.controller = controller;
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));

            String msg;
            while ((msg = in.readLine()) != null) {
                String[] ws = msg.split(" ");

                if (ws.length > 0) {
                    if (ws[0].equals("JOIN")) {
                        Integer port = Integer.parseInt(ws[1]);
                        this.controller.addDstore(new DstoreConnection(this.socket, port));
                    } else {
                        this.controller.addClient(new ClientConnection(this.socket, msg));
                    }

                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
