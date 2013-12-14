package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;


import common.messages.JSONSerializer;
import common.messages.KVMessage;
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
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
/*	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		} catch (IOException ioe) {
			logger.error("Connection could not be established!");

		} finally {
			// if(isRunning()) {
			// disconnect();
			// }
		}
	}*/

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
		// TODO Auto-generated method stub
		clientSocket = new Socket(mAddress, mPort);
		if(clientSocket!=null) {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			setRunning(true);
			logger.info("Connection established");
//			latestMsg = receiveMessage();
//				for (ClientSocketListener listener : listeners) {
//				listener.handleNewMessage(latestMsg);
//			}
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
			/*for (ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}*/
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
		// TODO Auto-generated method stub
		if (isRunning()) {
			try {
				if (value!=null && !value.equalsIgnoreCase("null")){
				    
				    TextMessage txtMsg = JSONSerializer.marshal(key, value,//error
                            StatusType.PUT);
				    logger.info("Sending : "+txtMsg.getMsg());
					sendMessage(txtMsg);
				}else{
				    TextMessage txtMsg = JSONSerializer.marshal(key, "", StatusType.PUT);
                    logger.info("Sending : "+txtMsg.getMsg());
					sendMessage(txtMsg);
				}
				TextMessage replyTxtMsg = receiveMessage();
				
				logger.info("Server response: "+replyTxtMsg.getMsg());
				
				return JSONSerializer.unMarshal(replyTxtMsg);
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

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		if (isRunning()) {
			try {
			    
			    TextMessage txtMsg = JSONSerializer.marshal(key, "", StatusType.GET);
                logger.info("Sending : "+txtMsg.getMsg());
			    
				sendMessage(txtMsg);
				TextMessage replyTxtMsg = receiveMessage();
				
				logger.info("Server response: "+replyTxtMsg.getMsg());
				
				return JSONSerializer.unMarshal(replyTxtMsg);
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
		logger.info("Receive message:\t '" + msg.getMsg() + "'");
		return msg;
	}

}
