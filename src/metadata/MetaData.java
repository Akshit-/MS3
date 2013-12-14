package metadata;

import java.util.List;

/**
 * 
 * @author Akshit
 *
 */
public class MetaData {
	
	String ip;
	String port;
	
	String rangeStart;
	String rangeEnd;
	
	public MetaData(String ip, String port, String rangeStart, String rangeEnd) {
		// TODO Auto-generated constructor stub
		
		this.ip = ip;
		this.port = port;
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;
		
	}

	public MetaData() {
		// TODO Auto-generated constructor stub
	}

	public String getIP() {
		return ip;
	}

	public void setIP(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getRangeStart() {
		return rangeStart;
	}

	public void setRangeStart(String rangeStart) {
		this.rangeStart = rangeStart;
	}

	public String getRangeEnd() {
		return rangeEnd;
	}

	public void setRangeEnd(String rangeEnd) {
		this.rangeEnd = rangeEnd;
	}
	
	
}
