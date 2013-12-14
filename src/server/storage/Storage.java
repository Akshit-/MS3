package server.storage;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


/**
 * Storage Module that uses ConcurrentHashMap<String,String> as data structure.
 * @author chrpapa
 * 
 */
public class Storage {

	/**
	 * data storage
	 */
	private static Storage s;

	/**
	 * logger
	 */
	private static Logger logger = Logger.getRootLogger();
	/*
	 * even though all operations are thread-safe, retrieval operations do not
	 * entail locking, and there is not any support for locking the entire table
	 * in a way that prevents all access. This class is fully interoperable with
	 * Hashtable in programs that rely on its thread safety but not on its
	 * synchronization details.
	 * 
	 * TODO this structure should be accessed only by using synchronized methods
	 * - DONE
	 */
	ConcurrentHashMap<String, String> data;

	/**
	 * Initializes the storage
	 */
	public static Storage init() {
		if (s == null) {
			s = new Storage();
			logger.debug("Storage class created");
		}
		return s;
	}

	public Storage() {
		data = new ConcurrentHashMap<String, String>();
	}

	/**
	 * Adds a kv pair to the storage
	 * 
	 * @param key
	 * @param value
	 * @return The previous value stored under that, in case of an update, null
	 *         otherwise
	 */
	public synchronized String put(String key, String value) {
		logger.debug("Storing kvpair: " + key + "," + value);
		return data.put(key, value);

	}

	/**
	 * Retrieve value
	 * 
	 * @param key
	 *            The key whose value wants to be found
	 * @return The value stored under that key. If no entry exists, null
	 */
	public synchronized String get(String key) {

		String result = data.get(key);
		logger.debug("Retrieved kvpair:" + key + "," + result);
		return result;

	}

	/**
	 * Remove value
	 * 
	 * @param key
	 *            The key whose value wants to be found
	 * @return The value stored under that key. If no entry exists, null
	 */
	public synchronized String delete(String key) {

		String result = data.remove(key);
		logger.debug("Removed kvpair:" + key + "," + result);
		return result;
	}
}
