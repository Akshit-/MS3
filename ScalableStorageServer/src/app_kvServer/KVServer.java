package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import logger.LogSetup;
import metadata.MetaData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import server.ECServerListener;
import server.ClientConnection;
import server.KVServerListener;
import server.storage.Storage;


public class KVServer extends Thread implements KVServerListener, ECServerListener{

	private static Logger logger = Logger.getRootLogger();

	public static int port;
	private ServerSocket serverSocket;
	private boolean running;
	private boolean isActiveForClients;
	private Storage storage;

	private static List<MetaData> mMetaDatas;
	private static MetaData mMetaData;

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
					ClientConnection connection = 
							new ClientConnection(client);

					connection.addKVServerListener(this);
					connection.addECServerListener(this);

					new Thread(connection).start();

					logger.info("Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getPort());
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
		return this.isActiveForClients;
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
	 * @return
	 */
	public static synchronized List<MetaData> getServiceMetaData() {
		return mMetaDatas;
	}

	/**
	 * 
	 * @param mMetaDatas
	 */
	public static synchronized void setServiceMetaData(List<MetaData> mMetaDatas_) {
		mMetaDatas = mMetaDatas_;
	}

	/**
	 * 
	 * @return
	 */
	public static synchronized MetaData getNodeMetaData() {
		return mMetaData;
	}

	/**
	 * 
	 * @param mMetaData
	 */
	public static synchronized  void setNodeMetaData(MetaData mMetaData_) {
		mMetaData = mMetaData_;
	}

	/**
	 * 
	 * @param metadata
	 * This function Initialize the KVServer with the meta-data and block it for client requests
	 */
	public void initKVServer(List<MetaData> metaDatas){

		setServiceMetaData(new ArrayList<MetaData>(metaDatas)); //store service metadata
		logger.info("initKVServer()");
		
		for(MetaData meta : mMetaDatas){
			if(meta.getPort().equals(Integer.toString(port))){
				setNodeMetaData(meta);// store node metadata
			}
		}
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
		this.isActiveForClients = true;
	}

	/** 
	 * This Stops the KVServer, all client requests are rejected and only ECS requests are processed.
	 * 
	 */
	public void stopKVServer(){
		this.isActiveForClients =false;
	}


	/**
	 * Function to Exit KVServer application.
	 * 
	 */
	public void shutDown(){
		//TODO
	}

	/**
	 * This function will Lock the KVServer for write operations.
	 * 
	 */
	public void lockWrite(){
		//TODO

	}

	/**
	 * This function will Lock the KVServer for write operations.
	 * 
	 */
	public void unlockWrite(){
		//TODO

	}

	/**
	 * This function will Transfer a subset (range) of the KVServer’s data to another.
	 * 
	 */
	public void moveData(String range, String server) {
		// TODO Auto-generated method stub

	}

	/**
	 * This function will Update the meta-data repository of this server.
	 * 
	 */
	public void update(MetaData metadata) {
		// TODO Auto-generated method stub

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
