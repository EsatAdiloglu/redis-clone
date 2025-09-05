import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RedisProcessor {
    private int getLength(InputStream in) throws IOException {
        int value = 0;
        int buf;
        while((buf = in.read()) != '\r'){
            if(buf == -1){
                throw new EOFException();
            }
            value = (value * 10) + (buf - '0');
        }
        //get rid of \n
        in.read();
        return value;
    }

    public List<Object> processMessage(InputStream in) throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            throw new EOFException();
        }
        
        if (firstByte == '*') {
            int length = getLength(in);
            List<Object> ret = new ArrayList<>(length);
            for(int i = 0; i < length; i++){
                ret.add(process(in));
            }
            return ret;
        } else {
            throw new UnknownError("Expected array, got: " + (char) firstByte);
        }
    }

    private Object process(InputStream in) throws IOException {
        int prefix = in.read();
        if(prefix == -1){
            throw new EOFException();
        }
        switch(prefix){
            case '$':
                return processMultiBulk(in);
            default:
                throw new UnknownError("Unknown: " + (char) prefix);
        }
    }
    private Object processMultiBulk(InputStream in) throws IOException {
        int length = getLength(in);
        byte[] readBuffer = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int readBytes = in.read(readBuffer, bytesRead, length - bytesRead);
            if (readBytes == -1) {
                throw new EOFException();
            }
            bytesRead += readBytes;
        }

        //get rid of \r\n
        in.read();
        in.read();

        return new String(readBuffer);
    }
}
