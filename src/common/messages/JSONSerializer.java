package common.messages;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;



import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.log4j.Logger;

import metadata.MetaData;
import common.messages.KVAdminMessage.Commands;
import common.messages.KVMessage.StatusType;

/**
 * 
 */
public class JSONSerializer {

	Logger logger = Logger.getRootLogger();
	/**
	 * 
	 * @param
	 * @return TextMessage
	 */
	public static TextMessage marshal(String key, String value,
			StatusType status) {
		KVMessageImpl msg = new KVMessageImpl(key, value, status);
		JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
		JsonObject object = objectBuilder.add("key", msg.getKey())
				.add("value", msg.getValue())
				.add("status", msg.getStatus().ordinal()).build();
		return new TextMessage(object.toString());
	}

	/**
	 * Used to send metadata to the client
	 * 
	 * @param
	 * @return TextMessage
	 */
	public static TextMessage marshal(KVMessage msg) {
		KVMessageImpl kvmsgimpl = (KVMessageImpl) msg;
		JsonObjectBuilder objectBuilder = Json.createObjectBuilder()
				.add("key", kvmsgimpl.getKey())
				.add("value", kvmsgimpl.getValue())
				.add("status", kvmsgimpl.getStatus().ordinal());

		if (kvmsgimpl.getMetaData() != null) {
			JsonArrayBuilder array = Json.createArrayBuilder();
			List<MetaData> list = kvmsgimpl.getMetaData();
			for (MetaData metadataItem : list) {
				array.add(Json.createObjectBuilder().add("ip", metadataItem.getIP())
						.add("port", metadataItem.getPort())
						.add("start", metadataItem.getRangeStart())
						.add("end", metadataItem.getRangeEnd()));
			}

			objectBuilder.add("metadata", array);
		}
		
		JsonObject value = objectBuilder.build();
		
		return new TextMessage(value.toString());
	}

	/**
	 * 
	 * @param
	 * @return KVMessageImpl
	 */
	public static KVMessageImpl unMarshal(TextMessage txtMsg) {
		String strMsg = txtMsg.getMsg();
		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();
		
		List<MetaData> metaDatas = new ArrayList<MetaData>();
		JsonArray jarray = jsonObject.getJsonArray("metadata");	

		if(jarray!=null){
			for(int i=0;i<jarray.size();i++){
				MetaData meta;

				meta = new MetaData(jarray.getJsonObject(i).getString("ip") 
						,jarray.getJsonObject(i).getString("port") 
						,jarray.getJsonObject(i).getString("start") 
						,jarray.getJsonObject(i).getString("end"));

				metaDatas.add(meta);
			}
		}
		
		return new KVMessageImpl(jsonObject.getString("key"),
				jsonObject.getString("value"),
				KVMessageImpl.getStatusType(jsonObject.getInt("status")),
				metaDatas);
	}

	public static TextMessage marshalKVAdminMsg(Commands command){

		JsonObjectBuilder builder = Json.createObjectBuilder()
				.add("command", command.ordinal());

		JsonObject value = builder.build();

		return new TextMessage(value.toString());


	}

	public static KVAdminMessageImpl unmarshalKVAdminMsgForCommand(TextMessage txtMsg){
		
		String strMsg = txtMsg.getMsg();
		System.out.println("Response Message : "+strMsg);
		
		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();
		
		
		KVAdminMessageImpl kvAdminMessage= new KVAdminMessageImpl();
		
		kvAdminMessage.setCommand(KVAdminMessageImpl.getCommandType(jsonObject.getInt("command")));
				
		return kvAdminMessage;		
		
		
	}
	
	
	public static TextMessage marshalKVAdminMsg(List<MetaData> list, Commands command, String destination, String range ){

		JsonObjectBuilder builder = Json.createObjectBuilder()
				.add("adminMsg", true)
				.add("command", command.ordinal())
				.add("range", range)
				.add("destination",destination);


		if(list!=null){
			JsonArrayBuilder array = Json.createArrayBuilder();

			for(MetaData item:list){
				array.add(Json.createObjectBuilder()
						.add("ip", item.getIP())
						.add("port", item.getPort())
						.add("start", item.getRangeStart())
						.add("end", item.getRangeEnd()));
			}

			builder.add("metadata", array);
		}

		JsonObject value = builder.build();
		
		return new TextMessage(value.toString());
	}

	public static KVAdminMessageImpl unmarshalKVAdminMsg(TextMessage txtMsg){
		
		String strMsg = txtMsg.getMsg();

		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();		

		List<MetaData> metaDatas = new ArrayList<MetaData>();
		JsonArray jarray = jsonObject.getJsonArray("metadata");	

		if(jarray!=null){
			for(int i=0;i<jarray.size();i++){
				MetaData meta;

				meta = new MetaData(jarray.getJsonObject(i).getString("ip") 
						,jarray.getJsonObject(i).getString("port") 
						,jarray.getJsonObject(i).getString("start") 
						,jarray.getJsonObject(i).getString("end"));

				metaDatas.add(meta);
			}
		}
		return new KVAdminMessageImpl(metaDatas
				,KVAdminMessageImpl.getCommandType(jsonObject.getInt("command"))
				,jsonObject.getString("range"),jsonObject.getString("destination"));

	}
}
