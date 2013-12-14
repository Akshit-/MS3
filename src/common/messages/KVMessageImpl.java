package common.messages;

public class KVMessageImpl implements KVMessage {
	private String mKey;
	private String mValue;
	private StatusType mStatusType;
	
	public KVMessageImpl(String key,String value,StatusType statusType) {
		// TODO Auto-generated constructor stub
		mKey=key;
		mValue=value;
		mStatusType=statusType;
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
			default:
				return StatusType.DELETE_ERROR;
		}
	}

}
