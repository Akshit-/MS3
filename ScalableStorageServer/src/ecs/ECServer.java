package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

import metadata.MetaData;

import common.messages.JSONSerializer;
import common.messages.KVAdminMessage.Commands;

public class ECServer {


	List<MetaData> mMetaData;
	List<ServerNodeData> mServerConfig;
	private List<Socket> mEcsClientSockets;

	JsonObjectBuilder objectBuilder = Json.createObjectBuilder();


	public ECServer(String string) {
		// TODO Auto-generated constructor stub
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
			// TODO Add logs
			e1.printStackTrace();

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

					System.out.println("Invalid ECS Config file : "+cmdLineConfig.getPath());
				}
			}
		} catch (IOException e) {
			// TODO Log this error
			e.printStackTrace();
		}
	}


	public void initService	(int numberOfNodes) {
		mMetaData = new ArrayList<MetaData>();
		MetaData tempMetaData = null;
		
		List<BigInteger> endPoints = new ArrayList<BigInteger>();
		
		HashMap<String, BigInteger> hashMap = new HashMap<String, BigInteger>();
		
		mEcsClientSockets = new ArrayList<Socket>();
		
		
		if(mServerConfig != null && numberOfNodes <= mServerConfig.size()){

			for(int i=0;i<numberOfNodes;i++){
				
				String ipPort = mServerConfig.get(i).getIPAddress()+":"+mServerConfig.get(i).getPort();
				
				BigInteger prevRangeBi = new BigInteger(getMD5(ipPort),16);
				
				hashMap.put(ipPort, prevRangeBi);

			}	
			

			ValueComparator vc = new ValueComparator(hashMap);
	       
			TreeMap<String ,BigInteger> sorted = new TreeMap<String, BigInteger>(vc);
			
			sorted.putAll(hashMap);
			
			
			int i = 1;
			
			
			MetaData previous = new MetaData();
			for (String key : sorted.keySet()) {
				
				System.out.println(key + " : " + sorted.get(key)); // why null values here?
				
	            tempMetaData = new MetaData();
				String tokens[] = key.split(":");
				
				tempMetaData.setIP(tokens[0]);
				tempMetaData.setPort(tokens[1]);
				
				//TODO change range values back to HEX after completion
				//tempMetaData.setRangeEnd(sorted.get(key).toString(16));
				tempMetaData.setRangeEnd(sorted.get(key).toString());
				
				if(i>1){
					
					BigInteger prevRangeBi = new BigInteger(previous.getRangeEnd(),16);
					BigInteger nextRangeBi = prevRangeBi.add(new BigInteger("1"));
					//TODO change range values back to HEX after completion					
					//tempMetaData.setRangeStart(nextRangeBi.toString(16));
					tempMetaData.setRangeStart(nextRangeBi.toString());
					
				}
				mMetaData.add(tempMetaData);
				previous = tempMetaData;
				i++;
				
	        }
			
			BigInteger prevRangeBi = new BigInteger(mMetaData.get(mMetaData.size()-1).getRangeEnd(),16);
			BigInteger nextRangeBi = prevRangeBi.add(new BigInteger("1"));
			
			//TODO change range values back to HEX after completion
			//mMetaData.get(0).setRangeStart(nextRangeBi.toString(16));
			mMetaData.get(0).setRangeStart(nextRangeBi.toString());
			
	        System.out.println(sorted.values()); // But we do have non-null values here!
			
	       
			String cmd= null;
			for(MetaData metaData: mMetaData ){
				 System.out.println("Now="+new BigInteger(metaData.getRangeStart(),16)+":"+new BigInteger(metaData.getRangeEnd(),16));
				
				 
				 
				 cmd = "java -jar ms3-server.jar "+metaData.getPort();
				//cmd = "ssh -n "+metaData.getIP()+" nohup java -jar ms3-server.jar "+metaData.getPort()+" ERROR &";
				Process process = null;
				Runtime run = Runtime.getRuntime();
				try {
					process = run.exec(cmd);
					
					Thread.sleep(1000);
					
					Socket ecsClientSocket = new Socket(metaData.getIP(),Integer.parseInt(metaData.getPort()));
					
					mEcsClientSockets.add(ecsClientSocket);
					
				} catch (IOException e) {
					//TODO Log this error
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			sendKVServersMetaData();
		}


	}

	public void start(){
		// TODO convert this command in EcsAdminMsg and send to all servers
		for(Socket socket: mEcsClientSockets){
			try {
				OutputStream socketOutputStream = socket.getOutputStream();
				// TODO create start command as  
				//				socketOutputStream.write(arg0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void stop(){
		for(Socket socket: mEcsClientSockets){
			try {
				OutputStream socketOutputStream = socket.getOutputStream();
				// TODO create stop command as  
				//				socketOutputStream.write(arg0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}	
	}

	public void shutDown(){
		for(Socket socket: mEcsClientSockets){
			try {
				OutputStream socketOutputStream = socket.getOutputStream();
				// TODO create shutdown command as  
				//				socketOutputStream.write(arg0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}	
	}

	private void sendKVServersMetaData(){
		for(Socket socket : mEcsClientSockets){
			try {
				OutputStream socketOutputStream = socket.getOutputStream();
				
				socketOutputStream.write(JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.INIT,"" , "").getMsgBytes());
				
				Thread.sleep(1000);
				
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void addNode(){
		if(mMetaData!=null && mServerConfig !=null && mMetaData.size() < mServerConfig.size()){
			MetaData metaData = new MetaData();
			metaData.setIP(mServerConfig.get(mMetaData.size()).getIPAddress());
			metaData.setPort(mServerConfig.get(mMetaData.size()).getPort());
			metaData.setRangeEnd(getMD5(mServerConfig.get(mMetaData.size()).getIPAddress()+":"+mServerConfig.get(mMetaData.size()).getPort()));
		}

	}

	public void removeNode(){

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
