package com.lseg.tools;

import java.util.TreeMap;

public class DataMessage {
	private String ric="";
	private int seqNo=0;
	private String source="";
	private long receivedMS=0;
	private TreeMap<Integer,String> fields=null;
	public DataMessage(String r, int sn, String s, long rMS, TreeMap<Integer,String> f) {
		ric = r;
		seqNo = sn;
		source = s;
		receivedMS = rMS;
		fields = (TreeMap<Integer, String>) f.clone();
	}
	public String getRIC() {
		return ric;
	}
	public String getSource() {
		return source;
	}
	public int  getSeqNo() {
		return seqNo;
	}
	public long getReceivedMS() {
		return receivedMS;
	}
	public TreeMap<Integer,String> getFields() {
		return fields;
	}
}
