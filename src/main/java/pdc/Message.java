package pdc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public Message(String magic, int version, String messageType, String studentId, long timestamp, byte[] payload) {
        this.magic = magic;
        this.version = version;
        this.messageType = messageType;
        this.studentId = studentId;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixing for each field and the entire message.
     */
    public byte[] pack() {
        byte[] magicBytes = magic != null ? magic.getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte[] typeBytes = messageType != null ? messageType.getBytes(StandardCharsets.UTF_8) : new byte[0];
        byte[] senderBytes = studentId != null ? studentId.getBytes(StandardCharsets.UTF_8) : new byte[0];
        
        int totalSize = 4 + // Total length
                        4 + magicBytes.length +
                        4 + // version
                        4 + typeBytes.length +
                        4 + senderBytes.length +
                        8 + // timestamp
                        4 + (payload != null ? payload.length : 0);

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(totalSize);
        buffer.putInt(magicBytes.length);
        buffer.put(magicBytes);
        buffer.putInt(version);
        buffer.putInt(typeBytes.length);
        buffer.put(typeBytes);
        buffer.putInt(senderBytes.length);
        buffer.put(senderBytes);
        buffer.putLong(timestamp);
        buffer.putInt(payload != null ? payload.length : 0);
        if (payload != null) {
            buffer.put(payload);
        }
        return buffer.array();
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) return null;
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int totalSize = buffer.getInt();
        if (data.length < totalSize) return null; // Incomplete data

        Message msg = new Message();
        
        int magicLen = buffer.getInt();
        byte[] magicBytes = new byte[magicLen];
        buffer.get(magicBytes);
        msg.magic = new String(magicBytes, StandardCharsets.UTF_8);
        
        msg.version = buffer.getInt();
        
        int typeLen = buffer.getInt();
        byte[] typeBytes = new byte[typeLen];
        buffer.get(typeBytes);
        msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);
        
        int senderLen = buffer.getInt();
        byte[] senderBytes = new byte[senderLen];
        buffer.get(senderBytes);
        msg.studentId = new String(senderBytes, StandardCharsets.UTF_8);
        
        msg.timestamp = buffer.getLong();
        
        int payloadLen = buffer.getInt();
        if (payloadLen > 0) {
            msg.payload = new byte[payloadLen];
            buffer.get(msg.payload);
        } else {
            msg.payload = new byte[0];
        }
        
        return msg;
    }
}
