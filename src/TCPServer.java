import java.net.*;

public abstract class TCPServer {

    private ServerSocket socket;

    public TCPServer(Integer port) throws Exception {
        this.socket = new ServerSocket(port);
    }

    protected abstract void onAccept(Socket socket);

    protected void start() {
        while (true) {
            try {
                onAccept(this.socket.accept());
                System.out.println("(i) New connection");
            } catch (Exception e) {
                System.out.println("Error: unable to accept client connection\n    (!) " + e.getMessage());
            }
        }
    }
}
