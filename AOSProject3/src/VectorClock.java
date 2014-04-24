import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@SuppressWarnings("serial")
public class VectorClock implements Serializable {
    private int[] v;

    public int[] getV() {
        return v;
    }

    public void setV(int[] v) {
        this.v = v;
    }

    int myId;
    int no_of_processes;

    public VectorClock(int numProc, int id) {
        myId = id;
        no_of_processes = numProc;
        v = new int[numProc];
        for (int i = 0; i < no_of_processes; i++)
            v[i] = 0;
    }

    public VectorClock() {
        // TODO Auto-generated constructor stub
    }

    public void displayClock() {

        for (int i = 0; i < v.length; i++) {
            System.out.print(v[i] + "\t");
        }
    }

    public synchronized void internalEvent() {
        synchronized (v) {
            v[myId] += 1;
        }

    }

    public void sendEvent() {
        // include the vector in the message
        // System.out.println("\nInside Send Event");

        // synchronized (v) {

        v[myId] += 1;

        // }

    }

    public void receiveEvent(int[] sentValue, int senderId) {

        // synchronized (v) {
        // System.out.println("\n Inside receive event");
        for (int i = 0; i < no_of_processes; i++)
            v[i] = Math.max(v[i], sentValue[i]);
        v[senderId] += 1;

        // }

    }

    public int convertToScalar() {
        int sum = 0;
        for (int i = 0; i < v.length; i++) {
            sum = sum + v[i];
        }
        return sum;
    }

    public static byte[] serialize(Object obj) throws IOException {
        ObjectOutputStream out;// = new ObjectOutputStream();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);
        out.writeObject(obj);
        return bos.toByteArray();
    }

    public static Object deserialize(byte[] obj) throws IOException,
            ClassNotFoundException {
        ObjectInputStream in;// = new ObjectOutputStream();
        ByteArrayInputStream bos = new ByteArrayInputStream(obj);
        in = new ObjectInputStream(bos);
        return in.readObject();
    }

}
