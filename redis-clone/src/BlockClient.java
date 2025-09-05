
import java.io.OutputStream;
import java.net.Socket;

public class BlockClient {
    private final Socket socket;
    private final OutputStream outputStream;
    private final Long timeOut;
    private boolean unblock = false;

    public BlockClient(Socket socket , OutputStream outputStream, Long timeOut){
        this.socket = socket;
        this.outputStream = outputStream;
        this.timeOut = (timeOut * 1000000) + System.nanoTime();
    }

    public Socket getSocket(){
        return socket;
    }

    public OutputStream getOutputStream(){
        return outputStream;
    }

    public Long getTimeout(){
        return timeOut;
    }

    public void setUnblock(boolean state){
        unblock = state;
    }
    public boolean isUnblock() { return unblock; }

    public boolean isTimeOut(){
        if(timeOut == 0){
            return false;
        }
        return timeOut < System.nanoTime();
    }

    public boolean isClosed(){
        return socket.isClosed();
    }
}
