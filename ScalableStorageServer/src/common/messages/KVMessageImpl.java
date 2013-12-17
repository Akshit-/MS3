package common.messages;

import java.util.List;

import metadata.MetaData;

public class KVMessageImpl implements KVMessage {
	private String mKey;
	private String mValue;
	private StatusType mStatusType;
	private List<MetaData> metadata;
	
	public List<MetaData> getMetaData() {
		return metadata;
	}

	public void setMetadata(List<MetaData> metadata) {
		this.metadata = metadata;
	}

	public KVMessageImpl(String key,String value,StatusType statusType) {
		// TODO Auto-generated constructor stub
		mKey=key;
		mValue=value;
		mStatusType=statusType;
	}
	
	public KVMessageImpl(String key,String value,StatusType statusType, List<MetaData> metadata) {
		// TODO Auto-generated constructor stub
		mKey=key;
		mValue=value;
		mStatusType=statusType;
		this.metadata = metadata;
	}

	@Override
	public String getKey() {
		// TODO Auto-generated method stub
		return mKey;
	}

	public void setKey(String key){
		mKey=key;
	}

	@Override
	public String getValue() {
		// TODO Auto-generated method stub
		return mValue;
	}
	
	public void setValue(String value){
		mValue=value;
	}
	

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return mStatusType;
	}

	public void setStatus(StatusType statusType){
		mStatusType=statusType;
	}
	
	/**
	 * TODO instead of this function I use an update version of the enum class
	 * @param status
	 * @return
	 */
	public static StatusType getStatusType(int status){
		
		switch(status){
			case 0: return StatusType.GET;
			case 1: return StatusType.GET_ERROR;
			case 2: return StatusType.GET_SUCCESS;
			case 3: return StatusType.PUT;
			case 4: return StatusType.PUT_SUCCESS;
			case 5: return StatusType.PUT_UPDATE;
			case 6: return StatusType.PUT_ERROR;
			case 7: return StatusType.DELETE_SUCCESS;
			case 8: return StatusType.PUT_UPDATE;
			case 9: return StatusType.PUT_ERROR;
			case 10: return StatusType.DELETE_SUCCESS;
			default:
				return StatusType.DELETE_ERROR;
		}
	}

}
