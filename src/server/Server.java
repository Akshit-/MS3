package server;

import java.util.List;

import metadata.MetaData;
/**
 * 
 * @author Chrysa
 * thread-safe lazy-initialization 
 *
 */
public class Server {

	private static class Loader {
		static Server INSTANCE = new Server();
	}

	private Server() {
	}

	public static Server getInstance() {
		return Loader.INSTANCE;
	}
	
	/**
	 * 
	 */
	private boolean isActiveForClients;

	/**
	 * 
	 */
	private boolean isLockWrite;

	/**
	 * 
	 */
	private List<MetaData> serviceMetaData;

	/**
	 * 
	 */
	private MetaData nodeMetaData;
	
	/**
	 * 
	 * 
	 */
	private boolean responding;
	
	public boolean isResponding() {
		return responding;
	}

	public void setResponding(boolean responding) {
		this.responding = responding;
	}

	public boolean isLockWrite() {
		return isLockWrite;
	}

	public void setLockWrite(boolean isLockWrite) {
		this.isLockWrite = isLockWrite;
	}
	
	public boolean isActiveForClients() {
		return isActiveForClients;
	}

	public void setIsActiveForClients(boolean isLockWrite) {
		this.isActiveForClients = isLockWrite;
	}
	
	
	public List<MetaData> getServiceMetaData() {
		return serviceMetaData;
	}

	public void setServiceMetaData(List<MetaData> serviceMetaData) {
		this.serviceMetaData = serviceMetaData;
	}

	public MetaData getNodeMetaData() {
		return nodeMetaData;
	}

	public void setNodeMetaData(MetaData nodeMetaData) {
		this.nodeMetaData = nodeMetaData;
	}

	
}
