import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements Runnable{
    private final Socket clientSocket;
    private final ConcurrentHashMap<Object, Object> store;
    private final ConcurrentHashMap<Object, Long> timeStore;
    private final ConcurrentHashMap<String, List<Object>> listStore;
    private final ConcurrentHashMap<String, Queue<BlockClient>> blockManager;

    public ClientHandler(Socket clientSocket, ConcurrentHashMap<Object, Object> store, ConcurrentHashMap<Object, Long> timeStore, ConcurrentHashMap<String, List<Object>> listStore, ConcurrentHashMap<String, Queue<BlockClient>> blockManager) {
        this.clientSocket = clientSocket;
        this.store = store;
        this.timeStore = timeStore;
        this.listStore = listStore;
        this.blockManager = blockManager;
    }

    private void executeCommand(List<Object> arr, OutputStream out) throws IOException {
        String command = ((String) arr.getFirst()).toUpperCase();
        switch(command){
            case "COMMAND":
                if (arr.size() > 1 && "DOCS".equalsIgnoreCase(((String) arr.get(1)))) {
                    out.write("+PONG\r\n".getBytes());
                } else {
                    out.write("+PONG\r\n".getBytes());
                }
                break;
            case "PING":
                out.write("+PONG\r\n".getBytes());
                break;
            case "ECHO":
                executeECHO(arr, out);
                break;
            case "SET":
                executeSET(arr, out);
                break;
            case "GET":
                executeGET(arr, out);
                break;
            case "RPUSH":
                executePUSH(arr, out, "RIGHT");
                break;
            case "LRANGE":
                executeLRANGE(arr, out);
                break;
            case "LPUSH":
                executePUSH(arr,out, "LEFT");
                break;
            case "LLEN":
                executeLLEN(arr, out);
                break;
            case "LPOP":
                executeLPOP(arr,out);
                break;
            case "BLPOP":
                executeBLPOP(arr, out);
                break;
            case "TYPE":
                executeTYPE(arr,out);
                break;
            default:
                break;

        }

    }

    private void executeECHO(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 2){
            out.write(("-ERR missing argument for ECHO\r\n".getBytes()));
            return;
        }
        String output = ((String) arr.get(1));
        out.write(("$"+output.length()+"\r\n"+output+"\r\n").getBytes());
    }

    private void executeSET(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 3){
            out.write(("-ERR missing either key or value for SET\r\n").getBytes());
            return;
        }
        Object setKey = arr.get(1);
        Object setValue = arr.get(2);
        store.put(setKey, setValue);

        //timestamp
        if(arr.size() > 3){
            Long setTime = Long.valueOf((String) arr.get(4));
            timeStore.put(setKey, System.nanoTime() + (setTime * 1000000));
        }

        out.write("+OK\r\n".getBytes());
    }

    private void executeGET(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 2){
            out.write(("-ERR missing key for GET\r\n").getBytes());
            return;
        }
        Object getKey = arr.get(1);
        if(timeStore.get(getKey) != null){
            if(timeStore.get(getKey) < System.nanoTime()){
                timeStore.remove(getKey);
                store.remove(getKey);
            }
        }
        String getValue = ((String) store.get(getKey));
        if(getValue == null){
            out.write(("$-1\r\n").getBytes());
        }
        if(getValue != null) {
            out.write(("$" + getValue.length() + "\r\n" + getValue + "\r\n").getBytes());
        }
    }

    private void executePUSH(List<Object> arr, OutputStream out, final String LeftOrRight) throws IOException {
        if (arr.size() < 3) {
            out.write(("-ERR missing list name or element(s) for RPUSH\r\n").getBytes());
            return;
        }
        String listName = ((String) arr.get(1));
        List<Object> listValue = listStore.computeIfAbsent(listName, k -> Collections.synchronizedList(new ArrayList<>()));

        synchronized (listValue) {
            if (LeftOrRight.equals("RIGHT")) {
                for (int i = 2; i < arr.size(); i++) {
                    listValue.add(arr.get(i));
                }
            } else {
                for (int i = 2; i < arr.size(); i++) {
                    listValue.addFirst(arr.get(i));
                }
            }
            out.write((":" + listValue.size() + "\r\n").getBytes());
            notifyBlockClients(listName);
        }
    }

    private void executeLRANGE(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 4){
            out.write(("-ERR missing list name or start index or end index for LRANGE").getBytes());
            return;
        }
        String listName = ((String) arr.get(1));
        List<Object> listValue = listStore.get(listName);

        if(listValue == null){
            out.write(("*0\r\n").getBytes());
            return;
        }

        int startValue = Integer.parseInt((String) arr.get(2));
        int endValue = Integer.parseInt((String) arr.get(3));

        synchronized (listValue) {
            if(startValue < 0){
                startValue = Math.max(0, listValue.size() + startValue);
            }
            if(endValue < 0){
                endValue = Math.max(0, listValue.size() + endValue);
            }

            if(startValue >= listValue.size() || startValue > endValue){
                out.write(("*0\r\n").getBytes());
                return;
            }
            if(endValue >= listValue.size()){
                endValue = listValue.size() - 1;
            }

            out.write(("*" + (endValue - startValue + 1) + "\r\n").getBytes());
            for (int i = startValue; i <= endValue; i++) {
                String rangeElem = ((String) listValue.get(i));
                out.write(("$" + rangeElem.length() + "\r\n" + rangeElem + "\r\n").getBytes());
            }
        }
    }

    private void executeLLEN(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 2){
            out.write(("-ERR missing list name for LLEN").getBytes());
            return;
        }
        String listName = ((String) arr.get(1));
        List<Object> listValue = listStore.get(listName);

        if(listValue == null){
            out.write((":0\r\n").getBytes());
            return;
        }
        synchronized (listValue) {
            out.write((":" + listValue.size() + "\r\n").getBytes());
        }
    }

    private void executeLPOP(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 2){
            out.write(("-ERR missing list name for LLEN").getBytes());
            return;
        }
        String listName = ((String) arr.get(1));
        List<Object> listValue = listStore.get(listName);

        synchronized (listValue) {
            if (listValue == null || listValue.isEmpty()) {
                out.write(("$-1\r\n").getBytes());
                return;
            }

            if (arr.size() == 3) {
                int popAmt = Integer.parseInt((String) arr.get(2));
                out.write(("*" + popAmt + "\r\n").getBytes());
                for (int i = 0; i < popAmt; i++) {
                    String poppedElement = (String) listValue.removeFirst();
                    out.write(("$" + poppedElement.length() + "\r\n" + poppedElement + "\r\n").getBytes());
                }
            } else {
                String poppedElement = (String) listValue.removeFirst();
                out.write(("$" + poppedElement.length() + "\r\n" + poppedElement + "\r\n").getBytes());
            }
            if (listValue.isEmpty()) {
                listStore.remove(listName);
            }
        }
    }

    private void executeBLPOP(List<Object> arr, OutputStream out) throws IOException {
        if(arr.size() < 3){
            out.write(("-ERR missing list name or timeout for BLPOP\r\n").getBytes());
            return;
        }
        String listName = ((String) arr.get(1));

        // Convert from seconds to milliseconds
        long timeOut = (long) (Float.parseFloat((String) arr.get(2)) * 1000);
        if(timeOut < 0){
            out.write(("-ERR timeout is a negative number\r\n").getBytes());
            return;
        }

        List<Object> listValue = listStore.get(listName);
        if(listValue != null) {
            synchronized (listValue) {
                if (!listValue.isEmpty()) {
                    String poppedElement = (String) listValue.removeFirst();
                    out.write(("*2\r\n").getBytes());
                    out.write(("$" + listName.length() + "\r\n" + listName + "\r\n").getBytes());
                    out.write(("$" + poppedElement.length() + "\r\n" + poppedElement + "\r\n").getBytes());
                    if(listValue.isEmpty()){
                        listStore.remove(listName);
                    }
                    return;
                }
            }
        }


        BlockClient blockClient = new BlockClient(clientSocket, out, System.currentTimeMillis() + timeOut);
        Queue<BlockClient> blockList = blockManager.computeIfAbsent(listName, k -> new LinkedList<>());

        synchronized (blockList) {
            blockList.add(blockClient);
        }


        synchronized (blockClient) {
            try {
                if(timeOut == 0){
                    while (!blockClient.isUnblock() && !blockClient.isClosed()) {
                        blockClient.wait();
                    }
                }
                else{
                long startTime = System.currentTimeMillis();
                long remainingTime = timeOut;

                while (remainingTime > 0  && !blockClient.isUnblock() && !blockClient.isClosed()) {
                    blockClient.wait(remainingTime);
                    long elapsed = System.currentTimeMillis() - startTime;
                    remainingTime = timeOut - elapsed;
                }
}
                synchronized (blockList) {
                    blockList.remove(blockClient);
                }

                if (blockClient.isUnblock() && !blockClient.isClosed()) {
                    listValue = listStore.get(listName);
                    if (listValue != null) {
                        System.out.println("list isn't null");
                        synchronized (listValue) {
                            if (!listValue.isEmpty()) {
                                String poppedElement = (String) listValue.removeFirst();
                                out.write(("*2\r\n").getBytes());
                                out.write(("$" + listName.length() + "\r\n" + listName + "\r\n").getBytes());
                                out.write(("$" + poppedElement.length() + "\r\n" + poppedElement + "\r\n").getBytes());
                                if (listValue.isEmpty()) {
                                    listStore.remove(listName);
                                }
                                return;
                            }
                        }
                    }
                    else{
                        return;
                    }
                }

                // Timeout or error occurred
                out.write(("$-1\r\n").getBytes());

            } catch (InterruptedException e) {
                synchronized (blockList) {
                    blockList.remove(blockClient);
                }
                out.write(("$-1\r\n").getBytes());
                Thread.currentThread().interrupt(); // Restore interrupt status
            }
        }
    }

    private void executeTYPE(List<Object> arr, OutputStream out) throws IOException{
        if(arr.size() < 2){
            out.write(("-ERR missing key for TYPE").getBytes());
            return;
        }

        String key = ((String) arr.get(1));

        synchronized (store){
            if(store.containsKey(key)){
                out.write(("+string\r\n").getBytes());
                return;
            }
            out.write(("+none\r\n").getBytes());
        }
    }


    private void notifyBlockClients(String listKey) throws IOException {
        Queue<BlockClient> blockList = blockManager.get(listKey);
        if(blockList != null && !blockList.isEmpty()){
            List<Object> listValue = listStore.get(listKey);
            if(listValue != null && !listValue.isEmpty()){
                synchronized (blockList) {
                    BlockClient blockClient = blockList.poll();
                    while (blockClient != null) {
                        if (!blockClient.isClosed() && !blockClient.isTimeOut()) {
                            synchronized (blockClient) {
                                blockClient.setUnblock(true);
                                blockClient.notify();
                            }
                            break;
                        }
                        // This client was closed, try the next one
                        blockClient = blockList.poll();
                    }
                }
            }
        }
    }
    @Override
    public void run() {
        try (Socket socket = clientSocket;
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream()
        ){
            System.out.println("Client " + clientSocket.getRemoteSocketAddress() + " has connected");
            RedisProcessor redisProcessor = new RedisProcessor();

            while(!Thread.currentThread().isInterrupted() && !socket.isClosed() && socket.isConnected()) {
                List<Object> processedMessage = redisProcessor.processMessage(inputStream);
                if(processedMessage == null){
                    break;
                }
                executeCommand(processedMessage, outputStream);
                outputStream.flush();
            }
        } catch(EOFException e){
            System.out.println("Client " + clientSocket.getRemoteSocketAddress() + " has disconnected");
            return;
        }  catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            return;
        }

    }
}


