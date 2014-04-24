import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

public class Project1 {
    public static final int MESSAGE_SIZE = 10;
    public static int no_of_nodes = 0;
    public static int processNo = 0;
    Map<Integer, NodeInfo> allNodes;
    Map<Integer, SctpChannel> clntSock;
    public static ConcurrentLinkedQueue<Message1> messageQueue = new ConcurrentLinkedQueue<Message1>();
    public static VectorClock vectorClock;
   
    
    

    Lock lock = new ReentrantLock();

    public static void main(String args[]) throws Exception {

        Project1 conn = new Project1();
        String argument = args[0];
        

        conn.readConfig(); // Read the config file

        

        conn.processNo = Integer.parseInt(argument);

        Project1.vectorClock = new VectorClock(Project1.no_of_nodes,
                Project1.processNo);
        

        conn.createConnections(conn.processNo); // To create connections,for the
                                                // given process no. accept
                                                // connections from higher nodes
                                                // and connect to lower nodes

        System.out
                .println("-------------------------------------------------------------------------");

        SendThread st = new SendThread(conn.no_of_nodes, conn.clntSock,
                conn.processNo, conn.lock);
        st.setConn(conn);

        RecvThread rt = new RecvThread(conn.no_of_nodes, conn.clntSock,
                conn.processNo, conn.lock);
        rt.setConn(conn);

        Thread send = new Thread(st);
        Thread recv = new Thread(rt);

        send.start();

        recv.start();
       if(Project1.processNo == 1)
       {
       Message1 msg = new Message1("Hello",Project1.processNo,2);
       msg.setVectorClock(Project1.vectorClock);
       messageQueue.add(msg);
       }
       Message1 msgBye = new Message1("bye",Project1.processNo,200);
       messageQueue.add(msgBye);


    	  
        try {
            send.join();

            recv.join();

            System.out.println("\n ********PROGRAM OVER***********");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

   

    public void readConfig() {
        allNodes = new HashMap<Integer, NodeInfo>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("./config/config.txt"));
            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                if (sCurrentLine.startsWith("#")) {
                    continue;
                }

                else {
                    String[] tokens = sCurrentLine.split(" ");
                    allNodes.put(Integer.parseInt(tokens[0]), new NodeInfo(
                            tokens[1].trim(), Integer.parseInt(tokens[2])));
                    no_of_nodes++;

                }
            }

        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (NumberFormatException e2) {
            e2.printStackTrace();
        } catch (NullPointerException e3) {
            e3.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public void createConnections(int processNo) {

        clntSock = new HashMap<Integer, SctpChannel>();

        SctpServerChannel serverSock = null;

        try {
            serverSock = SctpServerChannel.open();
            InetSocketAddress serverAddr = new InetSocketAddress(
                    allNodes.get(processNo).serverAddress,
                    allNodes.get(processNo).socketPort);
            serverSock.bind(serverAddr);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Connect to lower nodes
        for (int i = 0; i < processNo; ++i) {

            try {
                SctpChannel ClientSock;
                InetSocketAddress ServerAddr = new InetSocketAddress(
                        allNodes.get(i).serverAddress,
                        allNodes.get(i).socketPort);
                ClientSock = SctpChannel.open();
                ClientSock.connect(ServerAddr);
                clntSock.put(i, ClientSock);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        for (int i = 0; i < processNo; ++i) {
            System.out.println("Process " + i + " is up ");
        }

        System.out.println("Process " + processNo + " Joined now");

        // Accept connections from higher sockets
        if (processNo != (no_of_nodes - 1)) {
            System.out.println("Waiting for other nodes to join.");
        }

        for (int i = processNo + 1; i < no_of_nodes; ++i) {
            SctpChannel clientSock;
            try {
                clientSock = serverSock.accept();
                clntSock.put(i, clientSock);
                System.out.println("Process " + i + " has joined");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }

}
