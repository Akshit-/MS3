package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import metadata.MetaData;

import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import common.messages.JSONSerializer;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;
import common.messages.KVMessage.StatusType;

/**
 * KVStore module acts as a program library for client applications 
 * in general and encapsulates the complete functionality to use a KV 
 * storage service running somewhere on the Internet.
 *
 */
public class KVStore extends Thread implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private String mAddress;
	private int mPort;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private List<MetaData> metadata;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		mAddress = address;
		mPort = port;
		listeners = new HashSet<ClientSocketListener>();

	}

	/**
	 * Tries to establish connection to the server on address and port
	 * initialized in constructor This method must only be called after
	 * initializing instance with {@link Constructor}
	 * 
	 * @throws Exception
	 *             if unable to connect with servver
	 * 
	 */
	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(mAddress, mPort);
		if (clientSocket != null) {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			setRunning(true);
			logger.info("Connection established");
			// latestMsg = receiveMessage();
			// for (ClientSocketListener listener : listeners) {
			// listener.handleNewMessage(latestMsg);
			// }
		}
	}

	/**
	 * Disconnects from the currently connected server. This method must only be
	 * called after connection has been established.
	 */
	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			/*
			 * for (ClientSocketListener listener : listeners) {
			 * listener.handleStatus(SocketStatus.DISCONNECTED); }
			 */
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	/**
	 * Closes the input/output stream to and closes the client socket.
	 * 
	 * @throws IOException
	 */
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			if (input != null) {
				input.close();
				input = null;
			}

			if (output != null) {
				output.close();
				output = null;
			}
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	/**
	 * This method tells if the client thread is running or not.
	 * 
	 * @return true if thread is running else returns false
	 */
	public synchronized boolean isRunning() {
		return running;
	}

	/**
	 * This method sets client thread is running or not.
	 * 
	 * @param run
	 *            status to be set
	 */
	public synchronized void setRunning(boolean run) {
		running = run;
	}

	/**
	 * This method add listener for client incoming messages.
	 * 
	 * @param listener
	 */
	public void addListener(ClientSocketListener listener) {
		listeners.add(listener);
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {

		if (isRunning()) {
			try {
				if (value!=null && !value.equalsIgnoreCase("null")){
				    
				    TextMessage txtMsg = JSONSerializer.marshal(key, value,//error
                            StatusType.PUT);
				    logger.info("Sending : "+txtMsg.getMsg());
					sendMessage(txtMsg);
				} else {
					TextMessage txtMsg = JSONSerializer.marshal(key, "",
							StatusType.PUT);
					logger.info("Sending : " + txtMsg.getMsg());
					sendMessage(txtMsg);
				}
				return processReply(receiveMessage(), StatusType.PUT);
			} catch (IOException ioe) {
				tearDownConnection();
				logger.error("IOException! Unable to put value to KV server");
				throw new Exception("Unable to put value to KV server");
			}
		} else {
		    logger.error("Not connected to KV Server!");
			throw new Exception("Not connected to KV Server!");
		}
	}

	/**
	 * Processes the servers reply and transparently handles Client's response
	 * to Storage Service
	 * 
	 * @param reply
	 * @return
	 */
	private synchronized KVMessage processReply(TextMessage reply, StatusType reqStatus) {
		logger.info("Server response: " + reply.getMsg());
		KVMessageImpl replyMsg = JSONSerializer.unMarshal(reply);
		String key = replyMsg.getKey();
		StatusType status = replyMsg.getStatus();
		/**
		 * In this case, server sends a message
		 */
		if (status.equals(StatusType.SERVER_NOT_RESPONSIBLE)) {
			// store metadata
			this.metadata = replyMsg.getMetaData();
			for (MetaData meta : this.metadata) {

				if ( tupleBelongsToServer(meta, key)) {
					this.disconnect(); //disconnect from the server
					KVStore responsibleServerConn = new KVStore(meta.getIP(), //connect to the server responsible for the requested tuple
							Integer.parseInt(meta.getPort()));
					if (reqStatus.equals(StatusType.PUT)) {
						try {
							return responsibleServerConn.put(replyMsg.getKey(),
									replyMsg.getValue());
						} catch (Exception e) {
							logger.error("Unable to add Key-value pair on KVServer listening on"+meta.getPort());
						}
					} else {
						try {
							return responsibleServerConn.get(replyMsg.getKey());
						} catch (Exception e) {
							logger.error("Unable to get Key-value pair from KVServer listening on"+meta.getPort());
						}
					}
					break;
				}
			}
			// the same with server
			// TODO client need to decide to which server has to connect and
			// RE-TRY TO send the request
		} else if (status.equals(StatusType.SERVER_STOPPED)) {
			logger.info("server is stopped, the request was rejected");
		} else if (status.equals(StatusType.SERVER_WRITE_LOCK)) {
			System.out.println("Server locked for out, only get possible");
			logger.info("Server locked for out, only get possible");
		}
		return replyMsg;

	}


	/**
	 *  check whether the pair belongs to server's subset 
	 * if it doesn't belong return true
	 * else return false
	 * 
	 * @param metadata
	 * @param key
	 * @return true if the server is not in charge of the particular request
	 */
	private boolean tupleBelongsToServer(MetaData metaData, String key) {
		//TODO Chryssa check whether the tuple belongs to this server

		BigInteger key_ = new BigInteger(getMD5(key),16);
		if (key_.compareTo(new BigInteger(metaData.getRangeStart(),16)) >= 0 && 
				key_.compareTo(new BigInteger(metaData.getRangeEnd(),16)) <= 0) return true;
		return false;
	}
	

	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			logger.debug("not able to cypher key");
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
	@Override
	public KVMessage get(String key) throws Exception {
		if (isRunning()) {
			try {

				TextMessage txtMsg = JSONSerializer.marshal(key, "",
						StatusType.GET);
				logger.info("Sending : " + txtMsg.getMsg());

				sendMessage(txtMsg);
				return processReply(receiveMessage(), StatusType.GET);
			} catch (IOException ioe) {
			    logger.error("Unable to get value from KV server");
				throw new Exception("Unable to get value from KV server");
			}
		} else {
		    logger.error("Not connected to KV Server!");
			throw new Exception("Not connected to KV Server!");
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
		if (output != null) {
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
		} else {
			System.out.println("sendMessage-->output==null");
		}
		logger.info("Send message:\t '" + msg.getMsg() + "'");
	}

	private TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		
		logger.info("KVStore::Starting Receive message ="+clientSocket.getLocalPort());
		
		boolean reading = true;
		
		while (read != 13 && reading) {/* carriage return */
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

			/* only read valid characters, i.e. letters and numbers */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
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
		logger.info("KVStore::Receive message:\t '" + msg.getMsg() + "'"+"="+clientSocket.getLocalPort());
		return msg;
	}

}
