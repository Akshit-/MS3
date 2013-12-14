/**
 * 
 */
package server;

import java.util.List;

import metadata.MetaData;

/**
 * @author Akshit
 *
 */
public interface ECServerListener {
	
	/**
     * 
     * @param metadata
     * This function Initialize the KVServer with the meta-data and block it for client requests
     */
	public void initKVServer(List<MetaData> metaDatas);
	
	/**
     * This starts the KVServer, all client requests and all ECS requests are processed.
     * 
     */
	public void start();
	
	
	/** 
     * This Stops the KVServer, all client requests are rejected and only ECS requests are processed.
     * 
     */	
	public void stopKVServer();
	
	/**
     * Function to Exit KVServer application.
     * 
     */
	public void shutDown();
	
	
	 /**
     * This function will Lock the KVServer for write operations.
     * 
     */
	public void lockWrite();
	
	
	/**
     * This function will Lock the KVServer for write operations.
     * 
     */
	public void unlockWrite();
	
	/**
     * This function will Transfer a subset (range) of the KVServer’s data to another.
     * 
     */
	public void moveData(String range, String server);
	
	
	/**
	 * This function will Update the meta-data repository of this server.
	 * 
	 */
	public void update(MetaData metadata);

	/**
	 * Get KVServer current status for Client connection.
	 */
	public boolean isActiveForClients();
	
}
