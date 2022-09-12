package com.lseg.tools;

import java.util.TreeMap;

public class UpdateInfo {
	String receivedDateTime="";
	String ric="";
	int seqNum=0;
	TreeMap<String,String> fields=null;
	String id="";
	public UpdateInfo(String rdt, String r, int sn) {
		receivedDateTime = rdt;
		ric = r;
		seqNum = sn;
		id = ric + String.valueOf(seqNum);
		fields = new TreeMap<String,String>();
	}
	public void clear() {
		receivedDateTime = "";
		ric = "";
		seqNum=0;
		id="";
		if(fields!=null)
			fields.clear();
	}
	public void setAField(String fn, String v) {
		if(fields!=null)
			fields.put(fn, v);
	}
	public UpdateInfo(UpdateInfo source) {
		receivedDateTime = source.receivedDateTime;
		ric = source.ric;
		seqNum = source.seqNum;
		id = source.id;
		fields = new TreeMap<String,String>(source.fields);
	}
	public String getID() {
		return id;
	}
	public String getRIC() {
		return ric;
	}
	public String getDateTime() {
		return receivedDateTime;
	}
	public int getSeqNum() {
		return seqNum;
	}
	public TreeMap<String,String> getFields() {
		return fields;
	}
	public String getFieldValue(String fname) {
		return fields.get(fname);
	}
	public String getAllValues() {
		StringBuffer values = new StringBuffer("");
		for(String fidName:fields.keySet()) {
			values.append(fidName + "=" + fields.get(fidName) + ";");
		}
		return values.toString();
	}
	public boolean hasRequiredFields() {
		return (!fields.isEmpty());
	}
}
