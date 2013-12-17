package server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import metadata.MetaData;

import org.apache.log4j.Logger;

import app_kvServer.KVServer;
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

			// sendMessage(new TextMessage(
			// "Connection to MSRG KV server established: "
			// + clientSocket.getLocalAddress() + " / "
			// + clientSocket.getLocalPort()));

			while (isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();

					if (latestMsg == null) {
						break;
					}

					JsonObject jsonObject = null;

					try {
						jsonObject = Json.createReader(
								new StringReader(latestMsg.getMsg()))
								.readObject();
					} catch (Exception e) {
						logger.info("handlemsg+" + latestMsg + ">");
					}

					if (jsonObject == null) {
						// return null;
						break;
					}

					if (jsonObject.get("adminMsg") != null) {
						// handleKVAdminMsg

						KVAdminMessage kvAdminMessage = getKVAdminMessage(latestMsg);

						logger.info("AdminMessage Received");

						// marshal the response and send back

					} else if (mECServerListener.isActiveForClients()) {
						// handleKVMsg

						System.out.println("HERE2=");
						KVMessage msg = getKVMessage(latestMsg);
						if (msg != null) {//TODO Chryssa not_responsible message to client
							if (msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)) 
								sendMessage(new TextMessage(JSONSerializer
										.marshal(msg)));
							else
							System.out.println("HERE3=" + msg.getKey());
							JsonObjectBuilder objectBuilder = Json
									.createObjectBuilder();
							JsonObject object = objectBuilder
									.add("key", msg.getKey())
									.add("value", msg.getValue())
									.add("status", msg.getStatus().ordinal())
									.build();
							sendMessage(new TextMessage(object.toString()));
						} else {

							break;
						}

					} else {
						// TODO Chryssa send server_stopped message to KVClient
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
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" + clientSocket.getInetAddress().getHostAddress()
				+ ":" + clientSocket.getPort() + ">: '" + msg.getMsg() + "'");
	}

	private TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		System.out.println("BEFORE");
		byte read = (byte) input.read();
		System.out.println("After");
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

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

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
		logger.info("RECEIVE \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '" + msg.getMsg().trim() + "'"
				+ "=" + msgBytes + ",");
		return msg;
	}

	/**
	 * Processes the client requests and returns the corresponding server's reply
	 * @param request the client request
	 * @return server's reply accordingly to client's request
	 */
	public KVMessage getKVMessage(TextMessage request) {

		KVMessage kvmessage = JSONSerializer.unMarshal(request);
			// TODO Chryssa
			if (serverNotResponsible(kvmessage))
				return new KVMessageImpl(kvmessage.getKey(),
						kvmessage.getValue(),
						StatusType.SERVER_NOT_RESPONSIBLE, KVServer.getServiceMetaData());

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

		if (kvmessage.getStatus().equals(StatusType.PUT)) {

			if (kvmessage.getValue().isEmpty()) {

				String previous_value = mKVServerListener.delete(kvmessage
						.getKey());
				if (previous_value != null) {
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							previous_value, StatusType.DELETE_SUCCESS);
				} else {
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.DELETE_ERROR);
				}

			} else {

				String previous_value = mKVServerListener.put(
						kvmessage.getKey(), kvmessage.getValue());

				logger.info("PUT Success");

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
	 *  check whether the pair belongs to server's subset 
	 * if it doesn't belong return true
	 * else return false
	 * 
	 * @param kvmessage
	 * @return true if the server is not in charge of the particular request
	 */
	private boolean serverNotResponsible(KVMessage kvmessage) {
		//TODO Chryssa check whether the tuple belongs to this server

		BigInteger key = new BigInteger(getMD5(kvmessage.getKey()),16);
		if (key.compareTo(new BigInteger(KVServer.getNodeMetaData().getRangeStart(),16)) >= 0 && 
				key.compareTo(new BigInteger(KVServer.getNodeMetaData().getRangeEnd(),16)) <= 0) return true;
		return false;
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
	private KVAdminMessageImpl getKVAdminMessage(TextMessage replyMsg) {
		// TODO Auto-generated method stub

		logger.info("getKVAdminMessage()");

		KVAdminMessageImpl kvAdminMessage = JSONSerializer
				.unmarshalKVAdminMsg(replyMsg);

		logger.info("Command=" + kvAdminMessage.getCommand().ordinal());
		if (kvAdminMessage.getCommand().equals(Commands.INIT)) {
			logger.info("INIT Command=");
			mECServerListener.initKVServer(kvAdminMessage.getMetaDatas());

		}

		if (kvAdminMessage.getCommand().equals(Commands.START)) {
			mECServerListener.start();
		}

		if (kvAdminMessage.getCommand().equals(Commands.STOP)) {
			mECServerListener.stopKVServer();
		}

		if (kvAdminMessage.getCommand().equals(Commands.SHUTDOWN)) {

			if (clientSocket != null) {
				try {
					input.close();
					output.close();
					clientSocket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			System.exit(1);
		}

		if (kvAdminMessage.getCommand().equals(Commands.LOCK_WRITE)) {
			// lockWrite()
		}

		if (kvAdminMessage.getCommand().equals(Commands.UNLOCK_WRITE)) {
			// unLockWrite()
		}

		if (kvAdminMessage.getCommand().equals(Commands.MOVE_DATA)) {
			// moveData()
		}

		if (kvAdminMessage.getCommand().equals(Commands.UPDATE)) {
			// update()
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