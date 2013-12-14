/**
 * 
 */
package server;


/**
 * @author Akshit
 *
 */
public interface KVServerListener {
	
	public String put(String key, String value);
	public String get(String key);
	public String delete(String key);
	
}
