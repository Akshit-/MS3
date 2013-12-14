package ecs;

public class ServerNodeData {
	private String nodeName;
	private String ipAddress;
	private String port;
	
	public ServerNodeData(String nodeName, String ipAddress, String port){
		this.nodeName=nodeName;
		this.ipAddress=ipAddress;
		this.port=port;
	}
	
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getIPAddress() {
		return ipAddress;
	}
	public void setIPAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}

}
