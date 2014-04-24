import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;

public class RecvThread implements Runnable {
    public int nodeCnt;
    Map<Integer, SctpChannel> clntSock;
    public int currNode;
    Project1 conn;
    Lock lock;

    public RecvThread() {

    }

    private static String byteToString(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(10000);
        byte[] bufArr = new byte[byteBuffer.remaining()];
        byteBuffer.get(bufArr);
        return new String(bufArr);
    }

    public RecvThread(int nodeCnt, Map<Integer, SctpChannel> clntSock,
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
        int cnt = 0;

        while (true) {
            // lock.lock();
            ByteBuffer byteBuffer;
            byteBuffer = ByteBuffer.allocate(10000);

            try {

                for (int id : clntSock.keySet()) {
                    SctpChannel sock = clntSock.get(id);

                    byteBuffer.clear();
                    sock.configureBlocking(false);
                    MessageInfo messageInfo = sock.receive(byteBuffer, null,
                            null);
                    byteBuffer.flip();
                    // String message = byteToString(byteBuffer);
                    if (byteBuffer.remaining() > 0) {
                        Message1 receivedMsg = (Message1) deserialize(byteBuffer
                                .array());
                        String message = receivedMsg.getMsg();
                        String messageType = receivedMsg.getMessageType();
                        int senderNodeId = receivedMsg.getSenderId();
                        System.out.println("\nMessage received from "
                                + senderNodeId + "\t at(Self): " + currNode
                                + "\t message type is \t" + message);
                        System.out
                                .println("\nClock values received with the message");
                        synchronized (Project1.vectorClock) {
                            if (!message.equalsIgnoreCase("token")
                                    && !message.equalsIgnoreCase("Bye"))
                                receivedMsg.getVectorClock().displayClock();

                            if (messageInfo != null
                                    && (message.toString().trim().length() != 0)
                                    && (!message.equalsIgnoreCase("token"))
                                    && (!message.equalsIgnoreCase("Bye"))) {
                                Project1.vectorClock.receiveEvent(receivedMsg
                                        .getVectorClock().getV(), receivedMsg
                                        .getSenderId());
                            }
                            System.out
                                    .println("\nVector clock after receive event.....");
                            Project1.vectorClock.displayClock();
                            // System.out.println("\n Message received is \t"
                            // + message + "\n");
                            /*
                             * if
                             * (receivedMsg.getMsg().equalsIgnoreCase("token"))
                             * { System.out .println(
                             * "\n *************Token Received*************\n");
                             * System.out .println(
                             * "\n----------Fulfilled Requests Vector in the token---------\n"
                             * ); receivedMsg.token
                             * .displayfulfilledRequestsVector(); System.out
                             * .println(
                             * "\n------------Unfulfilled Requests Queue in the token------\n"
                             * ); receivedMsg.token.displayQueue();
                             * 
                             * }
                             */
                            if (receivedMsg.getMsg()!= null) {
                            	System.out.println("Received message from node \t"+ receivedMsg.getSenderId() +"\t by node \t"+receivedMsg.getReceiverId()+ "\t is \t" + message);
                                
                                

                                

                                
                            }
                            
                            // condition to check if i have received BYE
                            if (message.equalsIgnoreCase("Bye")) {
                                cnt++;
                                // if i have received BYE from all

                            }

                        }
                    }
                    byteBuffer.clear();
                }// end for

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
            } catch (NullPointerException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                // lock.unlock();
            }

            if (cnt == nodeCnt - 1) {
                // Thread.sleep(1000);
                // sock.close();
                return;
            }

        }

    }

    public static Object deserialize(byte[] obj) throws IOException,
            ClassNotFoundException {
        ObjectInputStream in;// = new ObjectOutputStream();
        ByteArrayInputStream bos = new ByteArrayInputStream(obj);
        in = new ObjectInputStream(bos);
        return in.readObject();
    }
}
