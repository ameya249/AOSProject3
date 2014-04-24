
public class NodeInfo {
	Integer socketPort;	
	String serverAddress;	

	
	public NodeInfo() {
		}
	
	public NodeInfo(String serverAddress,Integer socketPort) {
		this.socketPort = socketPort;
		this.serverAddress = serverAddress;
	}
	
	public String NodeDetails(){
		return "Port is " + this.socketPort + "Address is " + this.serverAddress;
	}
}
