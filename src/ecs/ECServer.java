package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.json.Json;
import javax.json.JsonObjectBuilder;


import org.apache.log4j.Logger;

import common.messages.JSONSerializer;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.Commands;
import common.messages.TextMessage;
import metadata.MetaData;

public class ECServer {

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	List<MetaData> mMetaData;
	List<ServerNodeData> mServerConfig;
	private List<Socket> mEcsClientSockets;
	private HashMap <String,Socket> mEcsClientSocketMap;
	JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
	private HashMap<String, BigInteger> hashMap;
	private TreeMap<String, BigInteger> sorted;



	private static Logger logger = Logger.getRootLogger();


	public ECServer(String string) {
		File cmdLineConfig = null;
		if(string!=null){
			cmdLineConfig = new File(string);
		} else {
			//Error
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(cmdLineConfig));
		} catch (FileNotFoundException e1) {
			logger.error("ECS Config file not fount at : "+cmdLineConfig.getPath());
			System.out.println("ECS Config file not fount at : "+cmdLineConfig.getPath());
//			e1.printStackTrace();

		}
		String line = null;
		String[] tokens = null;
		mServerConfig = new ArrayList<ServerNodeData>();
		try {
			while ((line = reader.readLine()) != null) {
				tokens=line.split(" ");
				if(tokens.length ==3){
					ServerNodeData tempServerConfig = new ServerNodeData(tokens[0],tokens[1],tokens[2]);
					mServerConfig.add(tempServerConfig);
				} else {
					// TODO Log this error
					logger.error("Invalid ECS Config file : "+cmdLineConfig.getPath());
					System.out.println("Invalid ECS Config file : "+cmdLineConfig.getPath());
				}
			}
		} catch (IOException e) {
			// TODO Log this error
			logger.error("IOException while reading the ecs config file.");
			System.out.println("IOException while reading the ecs config file.");
//			e.printStackTrace();
		}
	}

	public int getMaxAvailableNodeCount() {
		if(mServerConfig!=null) {
			return mServerConfig.size();
		} else {
			logger.debug("getMaxAvailableNodeCount()-->mServerConfig is NULL!!!");
			return 0;
		}
	}

	public int getActivatedNodeCount() {
		if(mMetaData!=null) {
			return mMetaData.size();
		} else {
			logger.debug("getActivatedNodeCount()-->mMetaData is NULL!!!");
			return 0;
		}
	}




	public boolean initService(int numberOfNodes) {
		boolean result = true;
		initMetaData(numberOfNodes);
		mEcsClientSockets = new ArrayList<Socket>();
		mEcsClientSocketMap = new HashMap<String, Socket>();
		String cmd= null;
		for(MetaData metaData: mMetaData ){
			try{
				execSSH(metaData);
				//Creating socket connection to newly started KVServer
				Socket ecsClientSocket = new Socket(metaData.getIP(),Integer.parseInt(metaData.getPort()));
				//Adding socket connection to hashmap and socket list for ease of access
				mEcsClientSocketMap.put(metaData.getIP()+":"+metaData.getPort(), ecsClientSocket);
				mEcsClientSockets.add(ecsClientSocket);

			} catch (IOException e) {
				result = false;
				logger.error("IOException while creating socket connection to new node : "+metaData.getIP()+":"+metaData.getPort());
				//e.printStackTrace();
			}
		}
		logger.info("New KVServers count = "+mEcsClientSockets.size());
		try {
			//Waiting for KVServer to create Client connection and initialize it
			Thread.sleep(1000);
			result = sendKVServersMetaData();
		} catch (InterruptedException e) {
			logger.error("InterruptedException while sleeping before sending metadat to all KV servers.");
			//e.printStackTrace();
			result =false;
		}

		return result;
	}

	public boolean start(){
		boolean result = true;
		for(Socket socket: mEcsClientSockets){
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.START, "","");
				sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("start()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());
		
				if(!responseKVAdminMsg.getCommand().equals(Commands.START_SUCCESS)) {
					result = false;
					break;
				} 

			} catch (IOException e) {
				logger.error("IOException while sending start command to KVServer:"
								+socket.getInetAddress().getHostAddress()
								+":"+socket.getPort());
				//e.printStackTrace();
				result =  false;
			} 
		}
		return result;
	}

	private boolean sendStartCommand(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {

			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.START, "","");
			sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("start(ip,port)-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.START_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			logger.error("IOException while sending start command to KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort());
			//e.printStackTrace();
			return false;
		}
	}



	public boolean stop(){
		boolean result = true;
		for(Socket socket: mEcsClientSockets){
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.STOP, "","");
				sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("stop()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());
	
				if(!responseKVAdminMsg.getCommand().equals(Commands.STOP_SUCCESS)) {
					result = false;
					break;
				} 

			} catch (IOException e) {
				logger.error("IOException while sending stop command to KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort());
				//e.printStackTrace();
				result = false;
			}
		}	
		return result;
	}

	public boolean shutDown(){
		boolean result = true;
		for(Socket socket: mEcsClientSockets){
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.SHUTDOWN, "","");
				sendMessage(socket, txtMsg);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("IOException while Shutting down KVServer:"
						+ socket.getInetAddress().getHostAddress()
						+ ":" + socket.getPort());
				//e.printStackTrace();
				result = false;
			} 
		}
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			logger.error("InterruptedException while sleeping after sending shutdown msg to all KVServers");
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		
		mEcsClientSocketMap.clear();
		mEcsClientSocketMap = null;
		mEcsClientSockets.clear();
		mEcsClientSockets=null;
		mMetaData.clear();
		mMetaData = null;
		return result;
	}

	private boolean shutDownOldNode(String ip , int port){
		boolean result = true;
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.SHUTDOWN, "","");
			sendMessage(socket, txtMsg);

		} catch (IOException e) {
			logger.error("IOException while Shutting down KVServer:"
					+ socket.getInetAddress().getHostAddress()
					+ ":" + socket.getPort());
			result=false;
			//e.printStackTrace();
		}
		return result;

	}

	private boolean sendKVServersMetaData(){
		boolean result = true;
		for(Socket socket : mEcsClientSockets) {
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.INIT, "","");
				sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("sendKVServersMetaData()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());
	
				if(!responseKVAdminMsg.getCommand().equals(Commands.INIT_SUCCESS)) {
					result = false;
					break;
				}

			} catch (IOException e) {
				logger.error("IOException while sending KVServer metaData init commmand to KVServer:"
						+ socket.getInetAddress().getHostAddress()
						+ ":" + socket.getPort());
				//e.printStackTrace();
				result = false;
			}

		}
		return result;
	}

	private boolean updateKVServersMetaData(){
		boolean result = true;
		for(Socket socket : mEcsClientSockets) {
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.UPDATE, "","");
				sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("updateKVServersMetaData()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());
				if(!responseKVAdminMsg.getCommand().equals(Commands.UPDATE_SUCCESS)) {
					result = false;
					break;
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				result = false;
			}

		}
		return result;
	}

	private boolean updateKVServerMetaData(String ip, int port) {

		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.UPDATE, "","");
			sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("updateKVServerMetaData(ip,port)-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.UPDATE_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	private boolean initNewNodeMetaData(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.INIT, "","");
			sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("initNewNodeMetaData()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.INIT_SUCCESS)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	private boolean setLockWrite(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.LOCK_WRITE, "","");
			sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("setLockWrite()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());
			if(responseKVAdminMsg.getCommand().equals(Commands.LOCK_WRITE_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

	}

	private boolean releaseLockWrite(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.UNLOCK_WRITE, "","");
			sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = receiveMessage(socket);		
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("releaseLockWrite()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.UNLOCK_WRITE_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.out.println("releaseLockWrite()=exception:"+e);
			return false;
		}

	}

	private boolean moveData(MetaData sourceMetaData, MetaData destMetaData) {
		Socket socket = mEcsClientSocketMap.get(sourceMetaData.getIP()+":"+sourceMetaData.getPort());
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.MOVE_DATA, 
					destMetaData.getIP()+":"+destMetaData.getPort(),
					destMetaData.getRangeStart()+":"+destMetaData.getRangeEnd());
			sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = receiveMessage(socket);		
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			
			logger.debug("moveData()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.MOVE_DATA_SUCCESS)) {
				return true;
			} else {
				logger.error("Received response  !MOVE_DATA_SUCCESSS from KVServer-"+socket.getInetAddress().getHostAddress()+":"+socket.getPort());
				return false;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
	}

	public boolean addNode(){

		List<MetaData> oldMetaData = mMetaData;
		MetaData newNodeMetaData = null;
		MetaData successorNodeMetaData = null;
		initMetaData(getActivatedNodeCount()+1);
		int i=0;

		logger.debug("Old MetaData = "+oldMetaData);
		logger.debug("New MetaData = "+mMetaData);




		for(MetaData metaData: oldMetaData){			
			if(!metaData.getIP().equals(mMetaData.get(i).getIP()) || !metaData.getPort().equals(mMetaData.get(i).getPort())){
				//This is the newly added node
				newNodeMetaData = mMetaData.get(i);
				successorNodeMetaData = mMetaData.get(i+1);
				break;
			}
			i++;
		}
		if(newNodeMetaData ==null){
			newNodeMetaData = mMetaData.get(mMetaData.size()-1);
			successorNodeMetaData = mMetaData.get(0);

		}
		try {
			execSSH(newNodeMetaData);
			logger.debug("Adding new socket for newly added server-"+newNodeMetaData.getIP()+":"+newNodeMetaData.getPort());
			Socket ecsClientSocket = new Socket(newNodeMetaData.getIP(),Integer.parseInt(newNodeMetaData.getPort()));
			mEcsClientSocketMap.put(newNodeMetaData.getIP()+":"+newNodeMetaData.getPort(), ecsClientSocket);
			mEcsClientSockets.add(ecsClientSocket);
		} catch (IOException e) {
			//TODO Log this error
			e.printStackTrace();
		}
		boolean result=false;
		result = initNewNodeMetaData(newNodeMetaData.getIP(), Integer.parseInt(newNodeMetaData.getPort()));
		if(result) {
			result = sendStartCommand(newNodeMetaData.getIP(), Integer.parseInt(newNodeMetaData.getPort()));
			if(result) {
				result = setLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
				if(result) {
					result = moveData(successorNodeMetaData, newNodeMetaData);
					if(result) {
						result = updateKVServersMetaData();
						if(result) {
							result = releaseLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
						}
					}
				}
			}

		}
		return result;

	}



	public boolean removeNode(){
		List<MetaData> oldMetaData = mMetaData;
		MetaData oldNodeMetaData = null;
		MetaData successorNodeMetaData = null;
		boolean result=false;
		if(oldMetaData.size()>1) {
			initMetaData(oldMetaData.size()-1);
			int i=0;
			for(MetaData metaData: mMetaData){
				if(!metaData.getIP().equals(oldMetaData.get(i).getIP()) || !metaData.getPort().equals(oldMetaData.get(i).getPort())){
					//This is the old removed node
					oldNodeMetaData = oldMetaData.get(i);
					successorNodeMetaData = metaData;
					//				if(i!=mMetaData.size()-1)
					//				successorNodeMetaData = oldMetaData.get(i+1);
					break;
				}
				i++;
			}
			if(oldNodeMetaData==null){
				oldNodeMetaData = oldMetaData.get(oldMetaData.size()-1);
				successorNodeMetaData = oldMetaData.get(0);
			}

			
			result = setLockWrite(oldNodeMetaData.getIP(), Integer.parseInt(oldNodeMetaData.getPort()));
			if(result) {
				//result = setLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
				if(result) {
					result = updateKVServerMetaData(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
					if(result) {
						result = moveData(oldNodeMetaData, successorNodeMetaData);
						if(result) {
							result = releaseLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
							if(result) {
								result = shutDownOldNode(oldNodeMetaData.getIP(), Integer.parseInt(oldNodeMetaData.getPort()));
							} if(result) {
								mEcsClientSockets.remove(mEcsClientSocketMap.get(oldNodeMetaData.getIP()+":"+oldNodeMetaData.getPort()));
								mEcsClientSocketMap.remove(oldNodeMetaData.getIP()+":"+oldNodeMetaData.getPort());
								result = updateKVServersMetaData();	
							}
						}
					}
				}

			}

			
		} else {
			result = shutDown();
			
		}
		return result;
	}

	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			// TODO : Add logs
			return null;
		}
		messageDigest.reset();
		messageDigest.update(msg.getBytes());
		byte[] hashValue = messageDigest.digest();
		BigInteger bigInt = new BigInteger(1,hashValue);
		String hashHex = bigInt.toString(16);
		// Now we need to zero pad it if you actually want the full 32 chars.
		while(hashHex.length() < 32 ){
			hashHex = "0"+hashHex;
		}
		return hashHex;
	}

	private void initMetaData(int nodeCount){
		mMetaData = new ArrayList<MetaData>();
		MetaData tempMetaData = null;
		hashMap = new HashMap<String, BigInteger>();
		for(int i=0;i<nodeCount;i++){
			String ipPort = mServerConfig.get(i).getIPAddress()+":"+mServerConfig.get(i).getPort();
			BigInteger prevRangeBi = new BigInteger(getMD5(ipPort),16);
			hashMap.put(ipPort, prevRangeBi);
		}
		//Sort the meta data so that we can find adjacent node based on their hashvalues
		ValueComparator vc = new ValueComparator(hashMap);
		sorted = new TreeMap<String, BigInteger>(vc);
		sorted.putAll(hashMap);

		int i = 1;
		MetaData previous = new MetaData();
		for (String key : sorted.keySet()) {
			tempMetaData = new MetaData();
			String tokens[] = key.split(":");
			tempMetaData.setIP(tokens[0]);
			tempMetaData.setPort(tokens[1]);
			tempMetaData.setRangeEnd(sorted.get(key).toString(16));
			if(i>1){
				BigInteger prevRangeBi = new BigInteger(previous.getRangeEnd(),16);
				BigInteger nextRangeBi = prevRangeBi.add(new BigInteger("1"));
				tempMetaData.setRangeStart(nextRangeBi.toString(16));
			}
			mMetaData.add(tempMetaData);
			previous = tempMetaData;
			i++;

		}
		BigInteger prevRangeBi = new BigInteger(mMetaData.get(mMetaData.size()-1).getRangeEnd(),16);
		BigInteger nextRangeBi = prevRangeBi.add(new BigInteger("1"));
		mMetaData.get(0).setRangeStart(nextRangeBi.toString(16));
		logger.debug("New MetaData = "+mMetaData);
	}

	/**
	 * Method sends a TextMessage using the socket.
	 *
	 * @param socket
	 *            the socket that is to be used to sent the message.
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */
	private void sendMessage(Socket socket, TextMessage msg) throws IOException {
		OutputStream output = socket.getOutputStream();
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes);
		output.flush();
		logger.info("sendMessage() ="+msg.getMsg());
		logger.info("SEND \t<" + socket.getInetAddress().getHostAddress()
				+ ":" + socket.getPort() + ">: '" + msg.getMsgBytes() + "'");
	}

	private TextMessage receiveMessage(Socket socket) throws IOException {
		InputStream input = socket.getInputStream();
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		logger.info("receiveMessage() for:"+socket);
		byte read = (byte) input.read();
		boolean reading = true;
		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				logger.error("receiveMessage-->index == BUFFER SIZE");
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				logger.error("receiveMessage-->DROP SIZE reached");
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("RECEIVE \t<"
				+ socket.getInetAddress().getHostAddress() + ":"
				+ socket.getPort() + ">: '" + msg.getMsg()+ "'"
				+ "=" + msgBytes + ",");
		return msg;
	}

	private void execSSH(MetaData metaData) throws IOException{
		String cmd = "java -jar ms3-server.jar "+metaData.getPort();
//		String cmd = "ssh -n "+metaData.getIP()+" nohup java -jar ms3-server.jar "+metaData.getPort()+" ERROR &";
		Runtime run = Runtime.getRuntime();
		run.exec(cmd);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			logger.error("InterruptedException after execSSH command for KVServer:"+metaData.getIP()+":"+metaData.getPort());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//This class is used to sort the metadata using endrange value
	static class ValueComparator implements Comparator<String> {
		Map<String, BigInteger> base;
		ValueComparator(Map<String, BigInteger> base) {
			this.base = base;
		}

		@Override
		public int compare(String a, String b) {
			BigInteger x = base.get(a);
			BigInteger y = base.get(b);
			return x.compareTo(y);
		}
	}


}
