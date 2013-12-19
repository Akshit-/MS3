package common.messages;

import java.util.List;

import metadata.MetaData;

/**
 * @author Akshit
 *
 */
public interface KVAdminMessage {
	
	
	public enum Commands {
		INIT, 				/* Initialize KVServer */
		INIT_SUCCESS,		/* Initialize KVServer was success*/
		INIT_FAIL,			/* Initialize KVServer was failure*/
		START, 				/* Starts KVServer */
		START_SUCCESS, 		/* Starts KVServer was success*/
		START_FAIL, 		/* Starts KVServer was failure*/
		STOP, 				/* Stop KVServer */
		STOP_SUCCESS, 		/* Stop KVServer was success*/
		STOP_FAIL, 			/* Stop KVServer was failure*/
		SHUTDOWN, 			/* Exits the KVServer application */
		SHUTDOWN_SUCCESS, 	/* Exits the KVServer application was success*/
		SHUTDOWN_FAIL, 		/* Exits the KVServer application was failure*/
		LOCK_WRITE, 		/* Lock the KVServer for write operations */
		LOCK_WRITE_SUCCESS, /* Lock the KVServer for write operations was success*/
		LOCK_WRITE_FAIL, 	/* Lock the KVServer for write operations was failure*/
		UNLOCK_WRITE, 	    /* UnLock the KVServer for write operations */
		UNLOCK_WRITE_SUCCESS, 	/* UnLock the KVServer for write operations was success*/
		UNLOCK_WRITE_FAIL, 	/* UnLock the KVServer for write operations was failure*/
		MOVE_DATA, 		    /* Transfer a range of the KVServer’s data to another */
		MOVE_DATA_SUCCESS, 	/* Transfer a range of the KVServer’s data to another was success*/
		MOVE_DATA_FAIL, 	/* Transfer a range of the KVServer’s data to another was failure*/
		UPDATE, 			/* Update the meta-data repository of this server */
		UPDATE_SUCCESS, 	/* Update the meta-data repository of this server was success*/
		UPDATE_FAIL, 		/* Update the meta-data repository of this server was failure*/
		UNKNOWN				/*Unknown Command*/
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
