package common.messages;

import java.util.List;

import metadata.MetaData;

/**
 * @author Akshit
 *
 */
public interface KVAdminMessage {
	
	
	public enum Commands {
		INIT, 			/* Initialize KVServer */
		START, 			/* Starts KVServer */
		STOP, 			/* Stop KVServer */
		SHUTDOWN, 		/* Exits the KVServer application */
		LOCK_WRITE, 	/* Lock the KVServer for write operations */
		UNLOCK_WRITE, 	/* UnLock the KVServer for write operations */
		MOVE_DATA, 		/* Transfer a range of the KVServer’s data to another */
		UPDATE, 		/* Update the meta-data repository of this server */
	}
	
	/**
	 * @return the MetaData
	 * 
	 */
	public List<MetaData> getMetaDatas();
	
	/**
	 * 
	 * @return ECServer command
	 */
	public Commands getCommand();
	
	/**
	 * 
	 * @return Range of Hash for MoveData process
	 */
	public String getRange();
	
	/**
	 * 
	 * @return Destination KVServer Address where we are moving Data
	 */
	public String getDestinationAddress();
	
}
