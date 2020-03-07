package fault_tolerant_queue.src.main.java.com.distributed.queue;

import java.io.PrintWriter;
import java.net.*;
import java.util.*;

public class Client {
    private DatagramSocket socket;
    private InetAddress address; 
    private byte[] buf;
    private byte[] buf2;
    static PrintWriter printWriter = null;
    private static Scanner sc = new Scanner(System.in);
 
    public Client() {
        try {
            socket = new DatagramSocket();
            address = InetAddress.getByName("localhost");
            // printWriter = new PrintWriter(new FileWriter("./distributed_systems_assignment1/logger.txt",true));
            buf2 = new byte[1024];
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
 
    public String sendRequest(String msg, int port) {
        buf = msg.getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, port);
        String received = null;
        try {
            socket.send(packet);
            packet = new DatagramPacket(buf2, buf2.length);
            socket.receive(packet);
            received = new String(packet.getData(), 0, packet.getLength());
            // System.out.println("recv:"+received);
            // String[] values = received.split(":");
            // //System.out.println("request sent:"+requestSent+"received:"+requestReceive);
            // if(values.length < 2) {
            //     System.out.println(values.length);
            //     System.out.println("No response from server");
            // }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return received;        
    }
 
    public void close() {
        this.socket.close();
    }

    public static void main(String args[]) {
        Client udpClient = new Client();
        int choice1 = 0;
        int choice2 = 0;
        String msg = "";
        int port = 5000;
        boolean running = true;
        while(running) {
            System.out.println("Select the server you wish to connect to:");
            System.out.println("1. localhost:5000");
            System.out.println("2. localhost:5001");
            System.out.println("3. localhost:5002");
            System.out.println("4. localhost:5003");
            System.out.println("5. localhost:5004");
            System.out.println("6. Exit");
            choice1 = sc.nextInt();

            switch(choice1) {
                case 1 : 
                    port = 5000;
                    break;
                case 2 : 
                    port = 5001;
                    break;
                case 3 : 
                    port = 5002;
                    break;
                case 4 : 
                    port = 5003;
                    break;
                case 5 : 
                    port = 5004;
                    break;
                case 6 : 
                    System.exit(0);
                    break;    
                default :
                    System.out.println("Please enter a valid choice");
                    break;
            }

            if(choice1 >=1 && choice1 <=5) {
                while(true) {
                    System.out.println("Select the operation to be performed on the Queue:");
                    System.out.println("1. Queue create given label");
                    System.out.println("2. Queue destroy given queue id");
                    System.out.println("3. Queue Id with associated label");
                    System.out.println("4. Queue Push given id");
                    System.out.println("5. Queue Pop given id");
                    System.out.println("6. Queue Top given id");
                    System.out.println("7. Queue Size given id");
                    System.out.println("8. Exit");

                    choice2 = sc.nextInt();
                    int label = 0;
                    int queueId = 0;
                    int element = 0;
                    // message syntax to server - "queue <choice#> <parameters>"
                    switch(choice2) {
                        case 1 :
                            System.out.println("Enter the label of the queue:");
                            label = sc.nextInt();
                            msg = "queue 1 "+label;
                            break;
                        case 2 : 
                            System.out.println("Enter the Queue Id of the queue to be destroyed:");
                            queueId = sc.nextInt();
                            msg = "queue 2 "+queueId;
                            break;
                        case 3 : 
                            System.out.println("Enter the label of the queue:");
                            label = sc.nextInt();
                            msg = "queue 3 "+label;
                            break;
                        case 4 :
                            System.out.println("Enter the Queue Id of the queue:");
                            queueId = sc.nextInt();
                            System.out.println("Enter the element to be pushed into the queue:");
                            element = sc.nextInt();
                            msg = "queue 4 "+queueId +" "+element;
                            break;
                        case 5 : 
                            System.out.println("Enter the Queue Id of the queue:");
                            queueId = sc.nextInt();
                            msg = "queue 5 "+queueId;
                            break;
                        case 6 :
                            System.out.println("Enter the Queue Id of the queue:");
                            queueId = sc.nextInt();
                            msg = "queue 6 "+queueId;
                            break;
                        case 7 :
                            System.out.println("Enter the Queue Id of the queue:");
                            queueId = sc.nextInt();
                            msg = "queue 7 "+queueId;
                            break;
                        case 8 : 
                            System.exit(0);
                            break;    
                        default :
                            System.out.println("Please enter a valid choice");
                            break;
                    }

                   String response = udpClient.sendRequest(msg, port);
                   System.out.println(response);
                }
            }
        }
        udpClient.close();
        // try {        
        //     while(count < 120) {
        //         String response = udpClient.sendRequest("Sai Ram");
        //         TimeUnit.SECONDS.sleep(60);
        //         count ++;
        //         System.out.println(response);
        //     } 
        //     printWriter.close();     
        // }  catch(Exception e) {
        //     e.printStackTrace();
        // }
    }
}
