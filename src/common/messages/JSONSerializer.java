package common.messages;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;

import metadata.MetaData;
import common.messages.KVAdminMessage.Commands;
import common.messages.KVMessage.StatusType;

/**
 * 
 */
public class JSONSerializer {

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
	 * 
	 * @param
	 * @return KVMessageImpl
	 */
	public static KVMessageImpl unMarshal(TextMessage txtMsg) {
		String strMsg = txtMsg.getMsg();
		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();
		return new KVMessageImpl(jsonObject.getString("key"),
				jsonObject.getString("value"),
				KVMessageImpl.getStatusType(jsonObject.getInt("status")));
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

		//"metadata" , array.toString, "status", "any"

		return new TextMessage(value.toString());
	}

	public static KVAdminMessageImpl unmarshalKVAdminMsg(TextMessage txtMsg){
		String strMsg = txtMsg.getMsg();

		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();		
		
		List<MetaData> metaDatas = new ArrayList<MetaData>();
		
		JsonArray jarray = jsonObject.getJsonArray("metadata");
			
		
		for(int i=0;i<jarray.size();i++){
			MetaData meta;
						
			meta = new MetaData(jarray.getJsonObject(i).getString("ip") 
								,jarray.getJsonObject(i).getString("port") 
								,jarray.getJsonObject(i).getString("start") 
								,jarray.getJsonObject(i).getString("end"));
			
			metaDatas.add(meta);
		}	
		
		return new KVAdminMessageImpl(metaDatas
				,KVAdminMessageImpl.getCommandType(jsonObject.getInt("command"))
				,jsonObject.getString("range"),jsonObject.getString("destination"));

	}
}
