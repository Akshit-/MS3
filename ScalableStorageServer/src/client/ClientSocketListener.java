package client;

import common.messages.TextMessage;
/**
 * 
 * @author Akshit
 * This class is used to send messages to be printed on GUI
 */
public interface ClientSocketListener {

	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	public void handleNewMessage(TextMessage msg);
	
	public void handleStatus(SocketStatus status);
}
