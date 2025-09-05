import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class Main {
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(10);

    private static final ConcurrentHashMap<Object, Object> globalStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Object, Long> globalTimeStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<Object>> globalListStore = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Queue<BlockClient>> globalBlockManager = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Starting program");

        ServerSocket serverSocket = null;
        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);

            //So 'Address already in use' errors don't happen
            serverSocket.setReuseAddress(true);
            boolean serverListener = true;

            // Wait for connection from client.
            while (serverListener) {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(
                        new ClientHandler(
                                clientSocket,
                                globalStore,
                                globalTimeStore,
                                globalListStore,
                                globalBlockManager));
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
