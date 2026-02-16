package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Utility class for robust TCP message framing using length-prefix identifiers.
 * This ensures that messages are completely reconstructed even if TCP fragments them.
 */
public class NetworkUtils {

    /**
     * Writes an RPC message with a 4-byte length prefix.
     * [4-byte total length][Message payload]
     */
    public static void writeMessage(DataOutputStream out, Message msg) throws IOException {
        if (msg == null) return;
        byte[] data = msg.pack();
        synchronized (out) {
            out.write(data);
            out.flush();
        }
    }

    /**
     * Reads an RPC message by first reading the 4-byte length prefix and then
     * reading exactly that many bytes using readFully().
     */
    public static Message readMessage(DataInputStream in) throws IOException {
        try {
            // readInt() blocks until the 4-byte length prefix is ready
            int totalLength = in.readInt();
            
            // Validate length to prevent OOM or protocol issues
            if (totalLength < 4 || totalLength > 512 * 1024 * 1024) { // 512MB limit for jumbo payloads
                throw new IOException("Invalid message length: " + totalLength);
            }

            byte[] data = new byte[totalLength];
            ByteBuffer.wrap(data).putInt(totalLength);
            
            // readFully() blocks and guarantees that totalLength-4 bytes are read
            // This is the key to handling TCP fragmentation.
            in.readFully(data, 4, totalLength - 4);
            
            return Message.unpack(data);
        } catch (IOException e) {
            throw e;
        }
    }
}
