package common.messages;

import java.util.List;

import metadata.MetaData;

public class KVAdminMessageImpl implements KVAdminMessage {
	
	private List<MetaData> metaDatas;
	private Commands command;
	private String range;
	private String destinationAddress;
	
	public KVAdminMessageImpl(List<MetaData> metaDatas, Commands commands, String range, String destinationAddress) {
		this.metaDatas = metaDatas;
		this.command = commands;
		this.range = range;
		this.destinationAddress = destinationAddress;
	}
	
	public KVAdminMessageImpl(){
	
	}

	public Commands getCommand() {
		return command;
	}

	public void setCommand(Commands commands) {
		this.command = commands;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public String getDestinationAddress() {
		return destinationAddress;
	}

	public void setDestinationAddress(String ip_port) {
		this.destinationAddress = ip_port;
	}

	
	public List<MetaData> getMetaDatas() {
		return metaDatas;
	}
	
	
	public void setMetaDatas(List<MetaData> metaDatas) {
		this.metaDatas = metaDatas;
	}	
	
	public static Commands getCommandType(int command){
		
		switch(command){
			case 0: return Commands.INIT;
			case 1: return Commands.START;
			case 2: return Commands.STOP;
			case 3: return Commands.SHUTDOWN;
			case 4: return Commands.LOCK_WRITE;
			case 5: return Commands.UNLOCK_WRITE;
			case 6: return Commands.MOVE_DATA;
			default:
				return Commands.UPDATE;
		}
	}
	

}
