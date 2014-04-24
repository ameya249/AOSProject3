// Class to hold the message structure

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Message1 implements Serializable {

    private String msg;

    private VectorClock vectorClock;

    private int senderId;

    private int receiverId;

    private int scalarClock;
    
   

    public int getScalarClock() {
        return scalarClock;
    }

    public void setScalarClock(int scalarClock) {
        this.scalarClock = scalarClock;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(int receiverId) {
        this.receiverId = receiverId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    private String messageType;

    public String getMessageType() {
        /*
         * if (this.msg.equalsIgnoreCase("bye")) { return "Bye"; } else { return
         * "Regular"; }
         */

        switch (this.msg.toLowerCase()) {
        case "regular":
            return "REGULAR";
        case "request":
            return "REQUEST";
        case "token":
            return "TOKEN";
        case "bye":
            return "BYE";
        default:
            return "UNKNOWN";
        }

    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    Message1(String m, int senderId, int receiverId) {
        this.msg = m;
        this.senderId = senderId;
        this.receiverId = receiverId;
        
    }
    
    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg, int senderId) {
        this.msg = msg;
        this.senderId = senderId;
    }

    public VectorClock getVectorClock() {
        // VectorClock v = new VectorClock();
        // v =
        return this.vectorClock;
        // return v;
    }

    public synchronized void setVectorClock(VectorClock clock) {

        this.vectorClock = new VectorClock(Project1.no_of_nodes,
                Project1.processNo);
        this.vectorClock.setV(clock.getV().clone());
        // v.no_of_processes = Project1.no_of_nodes;
        // v.myId = Project1.processNo;
        // v = vectorClock;
        // this.vectorClock = vectorClock;
    }

    @Override
    public String toString() {

        int[] arr = this.getVectorClock().getV();

        String vc = "";

        for (int i = 0; i < arr.length; i++) {
            vc = vc + arr[i];
        }
        return "Node: " + this.getSenderId() + " Message: " + this.getMsg()
                + " Vector Clock: " + vc + " Scalar Value: " + getScalarClock();
    }

    public static byte[] serialize(Object obj) throws IOException {

        if (obj == null)
            return (new ByteArrayOutputStream().toByteArray());
        ObjectOutputStream out;// = new ObjectOutputStream();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        return bos.toByteArray();
    }

    public static Object deserialize(byte[] obj) throws IOException,
            ClassNotFoundException {

        if (obj == null)
            return new Object();
        ObjectInputStream in;// = new ObjectOutputStream();
        ByteArrayInputStream bos = new ByteArrayInputStream(obj);
        in = new ObjectInputStream(bos);
        return in.readObject();
    }

}
