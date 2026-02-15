package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {
    private String id;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public Worker() {
        String envId = System.getenv("STUDENT_ID");
        this.id = envId != null ? envId : "worker-" + java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            // Registration handshake
            Message regMsg = new Message("CSM218", 1, "REGISTER", id, System.currentTimeMillis(), null);
            sendMessage(regMsg);
            
            System.out.println("Worker " + id + " joined cluster at " + masterHost + ":" + port);
            
            listenForTasks();
        } catch (IOException e) {
            System.err.println("Worker failed to join cluster: " + e.getMessage());
        }
    }

    private void listenForTasks() {
        while (!socket.isClosed()) {
            try {
                Message msg = receiveMessage();
                if (msg == null) break;

                if ("TASK".equals(msg.messageType)) {
                    executor.submit(() -> handleTask(msg));
                } else if ("HEARTBEAT".equals(msg.messageType)) {
                    Message pong = new Message("CSM218", 1, "PONG", id, System.currentTimeMillis(), null);
                    sendMessage(pong);
                }
            } catch (IOException e) {
                System.err.println("Worker lost connection to master: " + e.getMessage());
                break;
            }
        }
    }

    private void handleTask(Message msg) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(msg.payload);
            int taskId = buffer.getInt();
            int rStart = buffer.getInt();
            int rEnd = buffer.getInt();
            int aRows = buffer.getInt();
            int aCols = buffer.getInt();
            int[][] A = new int[aRows][aCols];
            for (int i = 0; i < aRows; i++) {
                for (int j = 0; j < aCols; j++) {
                    A[i][j] = buffer.getInt();
                }
            }

            int bRows = buffer.getInt();
            int bCols = buffer.getInt();
            int[][] B = new int[bRows][bCols];
            for (int i = 0; i < bRows; i++) {
                for (int j = 0; j < bCols; j++) {
                    B[i][j] = buffer.getInt();
                }
            }

            // Perform multiplication for the assigned rows
            int resRows = aRows;
            int resCols = bCols;
            int[][] C = new int[resRows][resCols];
            for (int i = 0; i < resRows; i++) {
                for (int k = 0; k < aCols; k++) {
                    for (int j = 0; j < resCols; j++) {
                        C[i][j] += A[i][k] * B[k][j];
                    }
                }
            }

            // Pack result
            ByteBuffer resBuffer = ByteBuffer.allocate(4 + 4 + 4 + (resRows * resCols * 4));
            resBuffer.putInt(taskId);
            resBuffer.putInt(resRows);
            resBuffer.putInt(resCols);
            for (int i = 0; i < resRows; i++) {
                for (int j = 0; j < resCols; j++) {
                    resBuffer.putInt(C[i][j]);
                }
            }

            Message resMsg = new Message("CSM218", 1, "RESULT", id, System.currentTimeMillis(), resBuffer.array());
            sendMessage(resMsg);
        } catch (Exception e) {
            System.err.println("Error executing task: " + e.getMessage());
        }
    }

    private synchronized void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.write(data);
        out.flush();
    }

    private Message receiveMessage() throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        ByteBuffer.wrap(data).putInt(length);
        in.readFully(data, 4, length - 4);
        return Message.unpack(data);
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        String envPort = System.getenv("PORT");
        int port = envPort != null ? Integer.parseInt(envPort) : (args.length > 1 ? Integer.parseInt(args[1]) : 8080);
        new Worker().joinCluster(host, port);
    }
}
