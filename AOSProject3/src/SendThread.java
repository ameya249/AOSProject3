import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;

public class SendThread implements Runnable {

    public int nodeCnt;
    Map<Integer, SctpChannel> clntSock;
    public int currNode;
    Project1 conn;
    Lock lock;

    public SendThread() {

    }

    public SendThread(int nodeCnt, Map<Integer, SctpChannel> clntSock,
            int currNode, Lock lock) {
        this.nodeCnt = nodeCnt;
        this.clntSock = clntSock;
        this.currNode = currNode;
        this.lock = lock;
    }

    public void setConn(Project1 conn) {
        this.conn = conn;
    }

    @Override
    public void run() {
        // VectorClock oldclock = Project1.vectorClock;
        String msgStr = "Message";
        int i = 0;
        String msgType = "";

        // for loop to send messages to all others except self

        while (true) {
            while (!Project1.messageQueue.isEmpty()) {

                System.out.println("\nClock before Sending ");
                Project1.vectorClock.displayClock();
                Message1 msg = Project1.messageQueue.peek();
                if(msg.getMsg().equalsIgnoreCase("bye"))
                {
                	broadcast(msg);
                	Project1.messageQueue.remove();
                	break;
                }
                   

                    unicastMessage(msg.getReceiverId(), msg);
                    Project1.messageQueue.remove();
                    
                
                synchronized (Project1.vectorClock) {
                    msg.setVectorClock(Project1.vectorClock);
                }
                
                    synchronized (Project1.vectorClock) {
                        Project1.vectorClock.sendEvent();
                        System.out.println("\nClock After Sending \t");
                        Project1.vectorClock.displayClock();
                    }
                

                

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                

               
           // if (msg.getMsg().equalsIgnoreCase("Bye")) {
               //break;
           //}
        }

        

}
    }
    
    void broadcast(Message1 msg)
    {
    	for(int id : clntSock.keySet()){

     	   
     	   try {
     	   if(id!=currNode)
     	   {
     	     sendMessage(clntSock.get(id),msg);
     	     System.out.println("Sending bye");
     	   
     	   }
     	   else
     	   {
     	   continue;
     	   }
     	   } catch (CharacterCodingException e) {
     	   e.printStackTrace();
     	   } 
    }
    }

    void unicastMessage(int sendTo, Message1 msgToSend) {
        SctpChannel clientSocket = clntSock.get(new Integer(sendTo));
        System.out.println("\nSending token from process " + Project1.processNo
                + " to " + sendTo);
        try {
            sendMessage(clientSocket, msgToSend);

        } catch (CharacterCodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static void sendMessage(SctpChannel clientSock, Message1 Message)
            throws CharacterCodingException {

        // prepare byte buffer to send massage
        ByteBuffer sendBuffer = ByteBuffer.allocate(10000);
        sendBuffer.clear();

        // serialize the message
        byte[] serializedMsg = null;
        try {
            serializedMsg = serialize(Message);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // Reset a pointer to point to the start of buffer
        sendBuffer.put(serializedMsg);
        sendBuffer.flip();

        try {
            // Send a message in the channel
            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            clientSock.send(sendBuffer, messageInfo);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    public static byte[] serialize(Object obj) throws IOException {
        ObjectOutputStream out;// = new ObjectOutputStream();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        return bos.toByteArray();
    }

}
