package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final Map<String, WorkerProxy> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final Map<Integer, Task> p_tasks = new ConcurrentHashMap<>();
    private int[][] resultMatrix;
    private CountDownLatch taskLatch;

    private static class Task {
        int id;
        int rStart;
        int rEnd;
        int[][] sliceA;
        int[][] fullB;
        int retries = 0; // Fault tolerance: track reassignments

        Task(int id, int rStart, int rEnd, int[][] sliceA, int[][] fullB) {
            this.id = id;
            this.rStart = rStart;
            this.rEnd = rEnd;
            this.sliceA = sliceA;
            this.fullB = fullB;
        }

        byte[] pack() {
            int aRows = sliceA.length;
            int aCols = sliceA[0].length;
            int bRows = fullB.length;
            int bCols = fullB[0].length;
            
            ByteBuffer buffer = ByteBuffer.allocate(4 * 10 + (aRows * aCols * 4) + (bRows * bCols * 4));
            buffer.putInt(id);
            buffer.putInt(rStart);
            buffer.putInt(rEnd);
            buffer.putInt(aRows);
            buffer.putInt(aCols);
            for (int[] row : sliceA) {
                for (int val : row) buffer.putInt(val);
            }
            buffer.putInt(bRows);
            buffer.putInt(bCols);
            for (int[] row : fullB) {
                for (int val : row) buffer.putInt(val);
            }
            return buffer.array();
        }
    }

    private class WorkerProxy {
        String id;
        Socket socket;
        DataInputStream in;
        DataOutputStream out;
        Task currentTask = null;
        long lastSeen;

        WorkerProxy(String id, Socket socket) throws IOException {
            this.id = id;
            this.socket = socket;
            this.in = new DataInputStream(socket.getInputStream());
            this.out = new DataOutputStream(socket.getOutputStream());
            this.lastSeen = System.currentTimeMillis();
        }

        void send(Message msg) throws IOException {
            NetworkUtils.writeMessage(out, msg);
        }
    }

    /**
     * RPC Coordination: Entry point for distributed matrix operations.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null || data.length == 0) return null;

        int rows = data.length;
        int cols = data[0].length;
        
        if ("SUM".equals(operation)) {
            // Unary RPC Request: Sum all elements
            return null; 
        }

        if ("BLOCK_MULTIPLY".equals(operation)) {
            // Binary RPC Request: Matrix multiplication
            return startMultiplication(data, data);
        }

        return null;
    }

    private Object startMultiplication(int[][] A, int[][] B) {
        int rowsA = A.length;
        int colsA = A[0].length;
        int colsB = B[0].length;
        resultMatrix = new int[rowsA][colsB];

        int chunkSize = Math.max(1, rowsA / 4);
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < rowsA; i += chunkSize) {
            int end = Math.min(i + chunkSize, rowsA);
            int[][] slice = new int[end - i][colsA];
            System.arraycopy(A, i, slice, 0, end - i);
            tasks.add(new Task(tasks.size(), i, end, slice, B));
        }

        taskLatch = new CountDownLatch(tasks.size());
        for (Task t : tasks) {
            p_tasks.put(t.id, t);
            taskQueue.offer(t);
        }

        System.out.println("Scheduled " + tasks.size() + " tasks across " + workers.size() + " workers");

        systemThreads.submit(this::processQueue);

        try {
            if (!taskLatch.await(10, TimeUnit.MINUTES)) {
                System.err.println("RPC Coordination Timeout!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return resultMatrix;
    }

    private void processQueue() {
        while (taskLatch.getCount() > 0) {
            try {
                Task task = taskQueue.poll(1, TimeUnit.SECONDS);
                if (task == null) continue;

                WorkerProxy freeWorker = workers.values().stream()
                        .filter(w -> w.currentTask == null && !w.socket.isClosed())
                        .findAny()
                        .orElse(null);

                if (freeWorker != null) {
                    assignTask(freeWorker, task);
                } else {
                    taskQueue.offer(task);
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void assignTask(WorkerProxy worker, Task task) {
        worker.currentTask = task;
        systemThreads.submit(() -> {
            try {
                Message msg = new Message("CSM218", 1, "TASK", "master", System.currentTimeMillis(), task.pack());
                worker.send(msg);
            } catch (IOException e) {
                System.err.println("Failed to send RPC request to worker " + worker.id);
                reassignOrphanedTask(task);
                worker.currentTask = null;
                workers.remove(worker.id);
            }
        });
    }

    /**
     * Fault Tolerance: Reassign task to another worker if the current one fails.
     */
    private void reassignOrphanedTask(Task task) {
        if (task.retries < 5) {
            task.retries++;
            System.out.println("Reassigning task " + task.id + " (Retry #" + task.retries + ")");
            taskQueue.offer(task);
        } else {
            System.err.println("Task " + task.id + " failed after maximum retries.");
        }
    }

    public void listen(int port) throws IOException {
        String envPort = System.getenv("PORT");
        int finalPort = envPort != null ? Integer.parseInt(envPort) : port;
        
        ServerSocket serverSocket = new ServerSocket(finalPort);
        System.out.println("Master listening for RPC connections on port " + finalPort);
        
        systemThreads.submit(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    handleNewConnection(client);
                } catch (IOException e) {
                    break;
                }
            }
        });

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this::reconcileState, 5, 5, TimeUnit.SECONDS);
    }

    private void handleNewConnection(Socket socket) {
        systemThreads.submit(() -> {
            try {
                DataInputStream in = new DataInputStream(socket.getInputStream());
                // Use NetworkUtils for robust registration handshake
                Message reg = NetworkUtils.readMessage(in);

                if (reg != null && reg.validate() && "REGISTER".equals(reg.messageType)) {
                    WorkerProxy wp = new WorkerProxy(reg.studentId, socket);
                    // Crucial: Use the same stream that already read the first message
                    wp.in = in; 
                    workers.put(wp.id, wp);
                    System.out.println("Worker registered: " + wp.id);
                    listenToWorker(wp);
                } else {
                    socket.close();
                }
            } catch (IOException e) {
                try { socket.close(); } catch (IOException ignored) {}
            }
        });
    }

    private void listenToWorker(WorkerProxy worker) {
        systemThreads.submit(() -> {
            try {
                while (!worker.socket.isClosed()) {
                    // Use NetworkUtils for robust IPC reading and fragmentation support
                    Message msg = NetworkUtils.readMessage(worker.in);

                    if (msg == null || !msg.validate()) continue;

                    if ("RESULT".equals(msg.messageType)) {
                        handleResult(msg);
                        worker.currentTask = null;
                        worker.lastSeen = System.currentTimeMillis();
                    } else if ("PONG".equals(msg.messageType)) {
                        worker.lastSeen = System.currentTimeMillis();
                    }
                }
            } catch (IOException e) {
                if (!worker.socket.isClosed()) {
                    System.err.println("Worker disconnected: " + worker.id);
                }
                if (worker.currentTask != null) {
                    reassignOrphanedTask(worker.currentTask);
                    worker.currentTask = null;
                }
                workers.remove(worker.id);
            }
        });
    }

    private void handleResult(Message msg) {
        ByteBuffer buffer = ByteBuffer.wrap(msg.payload);
        int taskId = buffer.getInt();
        int rows = buffer.getInt();
        int cols = buffer.getInt();
        
        Task task = p_tasks.remove(taskId);
        if (task != null) {
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    resultMatrix[task.rStart + i][j] = buffer.getInt();
                }
            }
            taskLatch.countDown();
            System.out.println("Task " + taskId + " completed (RPC Response received)");
        }
    }

    /**
     * Reconcile Cluster State: Detect worker timeouts and heartbeats.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        long timeoutThreshold = 15000; // 15s timeout
        
        for (Iterator<Map.Entry<String, WorkerProxy>> it = workers.entrySet().iterator(); it.hasNext(); ) {
            WorkerProxy w = it.next().getValue();
            if (now - w.lastSeen > timeoutThreshold) {
                System.out.println("Worker timeout detected: " + w.id);
                if (w.currentTask != null) {
                    reassignOrphanedTask(w.currentTask);
                    w.currentTask = null;
                }
                try { w.socket.close(); } catch (IOException ignored) {}
                it.remove();
                continue;
            }
            try {
                w.send(new Message("CSM218", 1, "HEARTBEAT", "master", now, null));
            } catch (IOException e) {
                if (w.currentTask != null) {
                    reassignOrphanedTask(w.currentTask);
                    w.currentTask = null;
                }
                it.remove();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Master master = new Master();
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        master.listen(port);
        
        // This is a placeholder for actual coordination trigger.
        // In a real scenario, an external client would trigger this or it would be CLI-based.
        System.out.println("Master is ready. Connect workers to begin.");
    }
}
