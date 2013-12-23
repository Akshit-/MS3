package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import logger.LogSetup;
import metadata.MetaData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import server.ClientConnection;
import server.ECServerListener;
import server.KVServerListener;
import server.Server;
import server.storage.Storage;
import common.messages.JSONSerializer;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;


public class KVServer extends Thread implements KVServerListener, ECServerListener{

	private static Logger logger = Logger.getRootLogger();

	private static int port;
	private ServerSocket serverSocket;
	private boolean running;

	private Storage storage;

	private Server mServerData;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;


	/**
	 * Constructs a KV Server object which listens to connection attempts 
	 * at the given port.
	 * 
	 * @param port a port number which the Server is listening to in order to 
	 * 		establish a socket connection to a client. The port number should 
	 * 		reside in the range of dynamic ports, i.e 49152 – 65535.
	 */
	public KVServer(int port) {
		this.port = port;
		storage = Storage.init();
		mServerData = Server.getInstance();
	}

	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	public void run() {

		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					logger.info("KVServer:"+serverSocket.getInetAddress().getHostAddress()+":"+serverSocket.getLocalPort()+"Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getLocalPort());
					ClientConnection connection = 
							new ClientConnection(client);

					connection.addKVServerListener(this);
					connection.addECServerListener(this);
					new Thread(connection).start();

				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private boolean isRunning() {
		return this.running;
	}

	/**
	 * Get KVServer current status for Client connection.
	 */
	public boolean isActiveForClients() {
		return mServerData.isActiveForClients();
	}

	/**
	 * Check whether KVServer has been locked by Admin or not.
	 */
	public boolean isLockWrite() {
		return mServerData.isLockWrite();
	}


	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			System.out.println("Server listening on port: " 
					+ serverSocket.getLocalPort());
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * 
	 * @param metadata
	 * This function Initialize the KVServer with the meta-data and block it for client requests
	 */
	public void initKVServer(List<MetaData> metaDatas){

		mServerData.setServiceMetaData(new ArrayList<MetaData>(metaDatas)); //store service metadata
		List<MetaData> mMetaDatas = mServerData.getServiceMetaData();
		logger.info("initKVServer()");
		for(MetaData meta : mMetaDatas){
			if(meta.getPort().equals(Integer.toString(port))){
				mServerData.setNodeMetaData(meta);// store node metadata
			}
		}
		MetaData mMetaData = mServerData.getNodeMetaData();
		logger.info("Storing MetaData corresponding to this Server: "
				+mMetaData.getIP()
				+":"+mMetaData.getPort()
				+" "+mMetaData.getRangeStart()
				+" "+mMetaData.getRangeEnd());

	}

	/**
	 * This starts the KVServer, all client requests and all ECS requests are processed.
	 */
	public void startKVServer(){
		mServerData.setIsActiveForClients(true);
	}

	/** 
	 * This Stops the KVServer, all client requests are rejected and only ECS requests are processed.
	 * 
	 */
	public void stopKVServer(){
		mServerData.setIsActiveForClients(false);
	}


	/**
	 * This function will Lock the KVServer for write operations.
	 * 
	 */
	public void lockWrite(){
		mServerData.setLockWrite(true);
	}

	/**
	 * This function will unLock the KVServer for write operations.
	 * 
	 */
	public void unlockWrite(){
		mServerData.setLockWrite(false);
	}

	@Override
	public MetaData getNodeMetaData() {
		return mServerData.getNodeMetaData();
	}

	@Override
	public List<MetaData> getServiceMetaData() {
		return mServerData.getServiceMetaData();
	}	


	/**
	 * This function will move a subset (range) of the KVServer’s data to new Server
	 * @param server KVServer where data is to be moved.
	 * @return true if successfully moved else false.
	 * 
	 */
	public boolean moveData(String range, String server) {

		String ipPort[] = server.split(":");

		logger.info("KVServer::moveData() + Starting moveData process to new Server="+ipPort[1]);
				
		
		HashMap<String, String> dataToBeMoved = storage.getDataBetweenRange(range);

		if(dataToBeMoved.isEmpty()){
			logger.info("KVServer::moveData() + Nothing to be Moved!");
			return true;
		}

		Socket moveDataServer = null;		
		try {
			moveDataServer = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));
			logger.info("KVServer::moveData() + Socket created for KVServer="+ipPort[1]);
		} catch (IOException e) {
			logger.error("KVServer::moveData() + Error creating socket for New Server ="+e);
			return false;
		}

		for(Iterator<Entry<String, String>>it=dataToBeMoved.entrySet().iterator();it.hasNext();){

			Entry<String, String> entry = it.next();

			TextMessage txtMsg = JSONSerializer.marshal(entry.getKey(), entry.getValue(), 
					StatusType.PUT);

			logger.debug("KVServer::moveData() + Sending data to KVserver="+entry.getKey()+","+entry.getValue());

			try {				
				sendMessageToNewServer(moveDataServer, txtMsg);
				//Respone has to be PUT_SUCCESS
				TextMessage responseTxtMsg = receiveMessage(moveDataServer);

				KVMessage responseKVMsg = JSONSerializer.unMarshal(responseTxtMsg);


				if(responseKVMsg.getStatus()!=StatusType.PUT_SUCCESS ){
					logger.info("KVServer::moveData() + Couldn't move Data to new Server!");
					return false;

				}


			} catch (IOException e) {
				logger.error("KVServer::moveData() + Error while sending data to new Server : "+e);
				return false;
			}


		}
		
		deleteMovedData(dataToBeMoved);
		
		logger.info("KVServer::moveData() + Successfully moved Data to New Server!");

		return true;
	}

	private TextMessage receiveMessage(Socket clientSocket) throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		//		System.out.println("BEFORE");
		logger.info("receiveMessage() of KVSERVER :"+clientSocket.getLocalPort());

		InputStream input = clientSocket.getInputStream();

		byte read = (byte) input.read();

		//		System.out.println("After");
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			//			logger.info("while-->begin");
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				logger.info("while-->index == BUFFER_SIZE");
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
			//TODO need to check this logic
			/* only read valid characters, i.e. letters and constants */
			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				logger.error("DROP SIZE REACHED");
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
		logger.info("RECEIVE \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getLocalPort() + ">: '" + msg.getMsg().trim() + "'"
				+ "=" + msgBytes + ",");
		return msg;
	}



	/**
	 * This function deletes all the tuples that have been moved to new server.
	 * @param movedData data that was moved to new Server.
	 * @return true if successful else false
	 */
	private boolean deleteMovedData(HashMap<String, String> movedData) {

		return storage.deleteDataBetweenRange(movedData);
	}



	private void sendMessageToNewServer(Socket clientSocket, TextMessage msg) throws IOException {

		byte[] msgBytes = msg.getMsgBytes();
		OutputStream output = clientSocket.getOutputStream();

		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("KVServer::sendMessageToNewServer() + SEND KeyValue pairs to new Server \t<" + clientSocket.getInetAddress().getHostAddress()
				+ ":" + clientSocket.getLocalPort() + ">: '" + msg.getMsg() + "'");

	}

	/**
	 * This function will Update the meta-data repository of this server.
	 * 
	 */
	public void update(List<MetaData> metadatas) {
		initKVServer(metadatas);

	}

	/**
	 * 
	 * @param key
	 * @param value
	 * @return The previous value stored under that, in case of an update, null otherwise
	 */
	public String put(String key, String value){

		return storage.put(key, value);
	}

	/**
	 * @param key
	 * @return The previous value stored under that, in case of an update, null otherwise
	 */
	public String delete(String key){
		return storage.delete(key);
	}

	/**
	 * 
	 * @param key
	 * @return The value stored under that key. If no entry exists, null
	 */
	public String get(String key){				
		return storage.get(key);
	}

	private static String setLevel(String levelString) {

		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	/**
	 * Main entry point for the echo server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server/server.log", Level.ALL);

			if(args.length < 1 && args.length>2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: KVServer <port> <logLevel>!");
				System.out.println("Usage: <logLevel> is optional. Possible levels <ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF>");
				System.exit(1);
			} else {
				if(args.length==1) {
					port = Integer.parseInt(args[0]);
					new KVServer(port).start();
				} else if(LogSetup.isValidLevel(args[1])) {
					port = Integer.parseInt(args[0]);
					setLevel(args[1]); 
					new KVServer(port).start(); 
				} else {
					System.out.println("Error! Invalid logLevel");
					System.out.println("Possible levels <ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF>");
					System.exit(1);
				}


			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: KVServer <port> <logLevel>!");
			System.out.println("Usage: KVServer <port> <logLevel>!");
			System.out.println("Usage: <logLevel> is optional. Possible levels <ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF>");
			System.exit(1);
		}
	}


}
