package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.log4j.Logger;

import common.messages.JSONSerializer;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.Commands;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

/**
 * Represents a connection end point for a particular client that is connected
 * to the server. This class is responsible for message reception and sending.
 * The class also implements the echo functionality. Thus whenever a message is
 * received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;


	private KVServerListener mKVServerListener;

	private ECServerListener mECServerListener;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();


			while (isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();
					logger.info("ClientConnection:: run()-->latestMsg ="+latestMsg);
					if (latestMsg == null) {
						logger.error("ClientConnection:: run()-->latestMsg == null");
						break;
					}

					JsonObject jsonObject = null;

					try {
						jsonObject = Json.createReader(new StringReader(latestMsg.getMsg()))
								.readObject();
					} catch (Exception e) {
						logger.error("ClientConnection:: Exception while converting txtMsg to jsonObject");
					}

					if (jsonObject == null) {
						//return null;
						logger.error("ClientConnection:: run()-->jsonObject == null");
						break;
					}

					if (jsonObject.get("adminMsg")!=null) {						
						logger.info("ClientConnection:: AdminMessage Received");
						KVAdminMessage msg = getKVAdminMessage(latestMsg);
						if(msg!=null){
							logger.info("ClientConnection:: Sending Message to Admin with command:"+msg.getCommand().toString());
							try{
								TextMessage message = JSONSerializer.marshalKVAdminMsg(msg.getCommand());
								logger.info("ClientConnection:: Sending from KVServer:"+message.getMsg().toString());
								sendMessage(message);
							}catch(Exception e){
								logger.info("ClientConnection:: Could not send from KVServer:"+e);
							}
						}else{
							break;
						}
					}else if(mECServerListener.isActiveForClients()) {
						logger.info("ClientConnection:: KVClient message Received.");	
						KVMessage msg = getKVMessage(latestMsg);	
						if (msg != null) {
							if (msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)){
								logger.info("ClientConnection::Sending SERVER_NOT_RESPONSIBLE back to KVClient");
								
								sendMessage(new TextMessage(JSONSerializer
										.Marshal(msg)));
							}else{
								logger.info("ClientConnection::Sending key,value pair to KVClient: "
													+msg.getKey()+","+msg.getValue());
								
								JsonObjectBuilder objectBuilder = Json
										.createObjectBuilder();
								JsonObject object = objectBuilder
										.add("key", msg.getKey())
										.add("value", msg.getValue())
										.add("status", msg.getStatus().ordinal())
										.build();
								sendMessage(new TextMessage(object.toString()));
							}
						} else {
							logger.error("ClientConnection:: msg = null");
							break;
						}


					}else{
						KVMessageImpl kvmessage = JSONSerializer
								.unMarshal(latestMsg);
						sendMessage(JSONSerializer
								.marshal(kvmessage.getKey(),
										kvmessage.getValue(),
										StatusType.SERVER_STOPPED));
					}

					/*
					 * connection either terminated by the client or lost due to
					 * network problems
					 */
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {
			logger.debug("run()-->finally: Closing socket and streams");
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}

	}

	/**
	 * Method sends a TextMessage using this socket.
	 * 
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes);
		output.flush();
		logger.info("ClientConnection:: SEND \t<" + clientSocket.getInetAddress().getHostAddress()
				+ ":" + clientSocket.getLocalPort() + ">: '" + msg.getMsg() + "'");
	}

	private TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		byte read = (byte) input.read();
		
		logger.info("ClientConnection:: starting to RECEIVE ("+clientSocket.getLocalPort()+")");
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			//			logger.info("while-->begin");
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
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
			
			//TODO Need to check this logic as it may miss some special chars
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				logger.info("while-->DROP SIZE REACHED");
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
			//			logger.info("while-->end");
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
		logger.info("ClientConnection:: RECEIVE \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getLocalPort() + ">: '" + msg.getMsg() + "'"
				+ "=" + msgBytes + ",");
		return msg;
	}

	/**
	 * Processes the client requests and returns the corresponding server's reply
	 * @param request the client request
	 * @return server's reply accordingly to client's request
	 */
	public KVMessageImpl getKVMessage(TextMessage request) {

		KVMessageImpl kvmessage = JSONSerializer.unMarshal(request);

		if (serverNotResponsible(kvmessage)){
			
			logger.info("ClientConnection::getKVMessage() + SERVER_NOT_RESPONSIBLE for key="+kvmessage.getKey());
			return new KVMessageImpl(kvmessage.getKey(),
					kvmessage.getValue(),
					StatusType.SERVER_NOT_RESPONSIBLE, mECServerListener.getServiceMetaData());
		}

		if(kvmessage.getStatus().equals(StatusType.GET)){

			String value = mKVServerListener.get(kvmessage.getKey());

			if (value != null) {
				// GET_SUCCESS
				kvmessage = new KVMessageImpl(kvmessage.getKey(), value,
						StatusType.GET_SUCCESS);
				logger.info("GET SUCCESS! Found key=" + kvmessage.getKey()
						+ " on Server with value=" + kvmessage.getValue());

			} else {
				// GET_ERROR
				kvmessage = new KVMessageImpl(kvmessage.getKey(), "",
						StatusType.GET_ERROR);
				logger.info("GET ERROR! Cannot find key=" + kvmessage.getKey()
						+ " on Server");
			}

		}
		
		logger.info("ClientConnection::getKVMessage()+ isLockWrite="+mECServerListener.isLockWrite());
		
		//We first check whether this KVServer has been locked for writing by Admin (ECSServer).
		if (!mECServerListener.isLockWrite() && kvmessage.getStatus().equals(StatusType.PUT)) {

			if (kvmessage.getValue().isEmpty()) {

				String previous_value = mKVServerListener.delete(kvmessage
						.getKey());
				if (previous_value != null) {
					logger.info("DELETE SUCCESS! Deleted key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							previous_value, StatusType.DELETE_SUCCESS);
				} else {
					logger.info("DELETE ERROR! key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.DELETE_ERROR);
				}

			} else {

				String previous_value = mKVServerListener.put(
						kvmessage.getKey(), kvmessage.getValue());

				if (previous_value != null) {
					// PUT_UPDATE
					// updated previous one
					logger.info("PUT SUCCESS! Updated key="
							+ kvmessage.getKey() + " with new value="
							+ kvmessage.getValue());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.PUT_UPDATE);

				} else {
					// PUT_SUCCESS
					// inserted new one
					logger.info("PUT SUCCESS! Inserted new key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.PUT_SUCCESS);

				}

			}

		}
		return kvmessage;

	}


	/**
	 * check whether the pair belongs to server's subset 
	 * if it doesn't belong return true
	 * else return false
	 * 
	 * @param kvmessage
	 * @return true if the server is not in charge of the particular request
	 */
	private boolean serverNotResponsible(KVMessage kvmessage) {

		//Corrected Logic
		BigInteger key = new BigInteger(getMD5(kvmessage.getKey()),16);

		BigInteger startServer = new BigInteger(mECServerListener.getNodeMetaData().getRangeStart(),16);
		BigInteger endServer = new BigInteger(mECServerListener.getNodeMetaData().getRangeEnd(),16);


		BigInteger maximum = new BigInteger("ffffffffffffffffffffffffffffffff",16);

		BigInteger minimum = new BigInteger("00000000000000000000000000000000",16);

		logger.info("ClientConnection::serverNotResponsible() + key="+key
				+", Server's start="+startServer
				+", Server's end="+endServer
				+", Maximum ="+maximum
				+", Minimum ="+minimum);

		if(startServer.compareTo(endServer)<0){
			if (key.compareTo(startServer) > 0 && 
					key.compareTo(endServer) <= 0){

				logger.info("ClientConnection::serverNotResponsible(start<end) + return false");
				return false;
			}
		}else{
			//startServer > endServer
			//keycheck1 = startServer to Maximum && keycheck2 = 0 to end 
			if((key.compareTo(startServer) > 0 && key.compareTo(maximum) <= 0 )
					|| (key.compareTo(minimum) >= 0 && key.compareTo(endServer) <= 0 )){

				logger.info("ClientConnection::serverNotResponsible(start > end) + return false");
				return false;
			}

		}
		logger.info("ClientConnection::serverNotResponsible() + return true");
		return true;
	}


	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			logger.info("Exception occurred in MD5: e="+ex);
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
	private KVAdminMessageImpl getKVAdminMessage(TextMessage replyMsg) {
		
		KVAdminMessageImpl kvAdminMessage = JSONSerializer
				.unmarshalKVAdminMsg(replyMsg);

		if(kvAdminMessage.getCommand().equals(Commands.INIT)){
			logger.info("Executing INIT Command for("+clientSocket.getLocalPort()+")");			
			mECServerListener.initKVServer(kvAdminMessage.getMetaDatas());

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.INIT_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.UPDATE)){
			logger.info("Executing UPDATE Command for("+clientSocket.getLocalPort()+")");			
			mECServerListener.initKVServer(kvAdminMessage.getMetaDatas());

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.UPDATE_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.START)){
			logger.info("Executing START Command for("+clientSocket.getLocalPort()+")");
			mECServerListener.startKVServer();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.START_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.STOP)){
			logger.info("Excecuting STOP Command for("+clientSocket.getLocalPort()+")");
			mECServerListener.stopKVServer();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.STOP_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.SHUTDOWN)){
			logger.info("Executing SHUTDOWN Command for("+clientSocket.getLocalPort()+")");
			
			if (clientSocket != null) {
				try {
					input.close();
					output.close();
					clientSocket.close();
				} catch (IOException e) {
					
					logger.info("Exception occurred while executing SHUTDOWN Command: e="+e);
				}

			}
			System.exit(1);
		}else if(kvAdminMessage.getCommand().equals(Commands.LOCK_WRITE)){
			logger.info("Executing LOCK_WRITE Command for("+clientSocket.getLocalPort()+")");
			mECServerListener.lockWrite();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.LOCK_WRITE_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.UNLOCK_WRITE)){
			logger.info("Executing UNLOCK_WRITE Command for("+clientSocket.getLocalPort()+")");
			
			mECServerListener.unlockWrite();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.UNLOCK_WRITE_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.MOVE_DATA)){
			logger.info("Executing MOVE_DATA Command for("+clientSocket.getLocalPort()+")");
			
			String range = kvAdminMessage.getRange();
			String server = kvAdminMessage.getDestinationAddress();
			
			logger.info("Executing MOVE_DATA Command for("+clientSocket.getLocalPort()+")"
			+", rangestart="+new BigInteger(range.split(":")[0],16)
			+", rangeend="+new BigInteger(range.split(":")[1],16));

			if(mECServerListener.moveData(range,server)){
				//MOVE_DATA_SUCCESS
				logger.info("Sending MOVE_DATA_SUCCESS for("+clientSocket.getLocalPort()+")");
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.MOVE_DATA_SUCCESS);

			}else{
				//MOVE_DATA_FAIL
				logger.info("Sending MOVE_DATA_FAIL for("+clientSocket.getLocalPort()+")");
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.MOVE_DATA_FAIL);
			}			

		}else{
			logger.info("UNKNOWN command received for("+clientSocket.getLocalPort()+")");
			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.UNKNOWN);
		}

		return kvAdminMessage;

	}

	public void addKVServerListener(KVServerListener listener) {
		mKVServerListener = listener;
	}
	public void addECServerListener(ECServerListener listener) {
		mECServerListener = listener;
	}

}