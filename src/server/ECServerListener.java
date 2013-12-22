/**
 * 
 */
package server;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
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
	public void startKVServer();
	
	
	/** 
     * This Stops the KVServer, all client requests are rejected and only ECS requests are processed.
     * 
     */	
	public void stopKVServer();
		
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
     * This function will move a subset (range) of the KVServer’s data to new Server
	 * @param server KVServer where data is to be moved.
	 * @return true if successfully moved else false.
     * 
     */
	public boolean moveData(String range, String server);
	
	/**
	 * This function will Update the meta-data repository of this server.
	 * 
	 */
	public void update(List<MetaData> metadatas);

	/**
	 * Get KVServer current status for Client connection.
	 */
	public boolean isActiveForClients();
	
	
	/**
	 * Check whether KVServer has been locked by Admin or not.
	 */
	public boolean isLockWrite();
	
	/**
	 * This function will return Server's Meta Data.
	 * @return metaData corresponding to Server.
	 */
	public MetaData getNodeMetaData();
	
	/**
	 * This function return list of all Meta Data's
	 * @return list of Meta Data's
	 */
	public List<MetaData> getServiceMetaData();
	
}
