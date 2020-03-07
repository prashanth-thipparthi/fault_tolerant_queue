package fault_tolerant_queue.src.main.java.com.distributed.queue;

import java.net.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Server extends Thread {

    private DatagramSocket socket;
    private boolean running;
    private byte[] buf = new byte[1024];
    private byte[] buf2 = new byte[1024];
    private int portNumber = 5000;
    private int message_id = 0;
    private List<Integer> portNumbers = null;
    private FTQueue queue = null;
    private int sequenceNo = 0;
    private Map<String,String> bufferedMsg = null;  //<portno#msg_id, update_queue_msg>
    private Map<String,String> SequenceNoBuffer = null; //

    public Server() {
        try {
            socket = new DatagramSocket(this.portNumber);
            List<Integer> portsList = Arrays.asList(5000,5001,5002,5003,5004);
            this.portNumbers.addAll(portsList);
            queue = new FTQueue();
            bufferedMsg = new HashMap<String,String>();
            SequenceNoBuffer = new HashMap<String,String>();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    public Server(int portNumber) {
        this.portNumber = portNumber;
        try {
            socket = new DatagramSocket(this.portNumber);
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
 
    public String updateQueue(String msg) {
        int label = 0;
        int queueId = 0;
        int element = 0;

        String[] choices = msg.split(" ");
        switch(Integer.parseInt(choices[1])) {
                case 1 :                    
                    label = Integer.parseInt(choices[2]);
                    return queue.qCreate(label);
                case 2 : 
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qDestroy(queueId);
                case 3 : 
                    label = Integer.parseInt(choices[2]);
                    return queue.qId(label);
                case 4 :
                    queueId = Integer.parseInt(choices[2]);
                    element = Integer.parseInt(choices[3]);
                    return queue.qPush(queueId, element);
                case 5 : 
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qPop(queueId);
                case 6 :
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qTop(queueId);
                case 7 :
                    queueId = Integer.parseInt(choices[2]);
                    return queue.qSize(queueId); 
                default :
                    return "Please enter a valid choice";
        }
    }

    public void brodcastToServers(String broadcastMsg) {
        try {
            /* broadcast the message to all the server */
            DatagramPacket packet = null;
            InetAddress address = InetAddress.getByName("localhost");            
            buf = broadcastMsg.getBytes();
            for(Integer port : this.portNumbers){
                if(port != this.portNumber) {
                    packet = new DatagramPacket(buf, buf.length, address, port);
                    socket.send(packet);
                }            
            }
        }catch(Exception e)    {
            e.printStackTrace();
        }
    }

    public void brodcastClientMsg(String received) {
        try {
            this.message_id += 1;
            String broadcastMsg = "broadcast "+this.portNumber+"#"+this.message_id;
            broadcastMsg = broadcastMsg + " " +received;
            brodcastToServers(broadcastMsg);
        }catch(Exception e)    {
            e.printStackTrace();
        }
    }

    public void broadcastSequenceNumberToServers() {
        brodcastToServers("SequenceNo " + this.portNumber+"#"+this.message_id +" "+Integer.toString(this.sequenceNo));
    }

    public void bufferTheMessage(String msg) {
        String parts[] = msg.split(" ");
        bufferedMsg.put(parts[1],parts[2]);
    }

    public void delivertheMessage(String msg) {
        String parts[] = msg.split(" ");
        int seqNo = Integer.parseInt(parts[2]);
        String messageId = parts[1];

        if(seqNo == this.sequenceNo+1) {
            updateQueue(bufferedMsg.get(parts[1]));
            this.sequenceNo +=1;
        }else{
            //buffer the current <sequence_no,queue_msg_update>
            //request_replay <>
        }
    }
    public void run() {
        running = true;
        String response = null;
        try {
            while (running) {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                InetAddress address = packet.getAddress();
                int port = packet.getPort();                
                String received = new String(packet.getData(), 0, packet.getLength());
                System.out.println("received: "+received);
                if (received.startsWith("queue")) {
                    brodcastClientMsg(received);
                    response = updateQueue(received); 
                    if((this.sequenceNo+1)%1000 == (this.portNumber)%5000) {                        
                        this.sequenceNo += 1;
                        broadcastSequenceNumberToServers();                        
                    }                   
                }else if(received.startsWith("broadcast")) {
                    bufferTheMessage(received);
                } else if(received.startsWith("SequenceNo")) {
                    delivertheMessage(received);
                }
                buf2 = response.getBytes();
                packet = new DatagramPacket(buf2, buf2.length, address, port);
                socket.send(packet);                
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        if(args.length < 1) {
            System.out.println("Sytntax for running the server: ./server <port_number>");
        }
        int port = Integer.parseInt(args[0]);
        Server udpServer = new Server(port);
        udpServer.start();
    }
}