package com.lseg.tools;

import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

public class DataInfo {
	String _fileName;
	long _startDateTimeMS;
	long _endDateTimeMS;
	TreeSet<String> _comparedRICs;
	boolean _isDone;
	public DataInfo(String fn) {
		_fileName = fn;
		_comparedRICs = new TreeSet<String>();
		_isDone = false;
		_startDateTimeMS=0;
		_endDateTimeMS=0;
		
	}
	public DataInfo(DataInfo source) {
		_fileName = source._fileName;
		_comparedRICs = new TreeSet<String>(source._comparedRICs);
		_isDone = source._isDone;
		_startDateTimeMS=source._startDateTimeMS;
		_endDateTimeMS=source._endDateTimeMS;
		
	}
	
	public void setFileName(String fn) {
		_fileName = fn;
	}
	public String getFileName() {
		return _fileName;
	}
	
	public void foundRIC(String fr) {
		if(_comparedRICs.contains(fr)==false)
			_comparedRICs.add(fr);
		
	}
	public TreeSet<String> getComparedRICs() {
		return _comparedRICs;
	}
	public boolean hasStartDateTimeMS() { 
		return (_startDateTimeMS>0);
	}
	public void setStartDateTimeMS(long st) {
		if(_startDateTimeMS==0)
			_startDateTimeMS = st;
	}
	public long getStartDateTimeMS() {
		return _startDateTimeMS;
	}
	public void setEndDateTimeMS(long et) {
			_endDateTimeMS = et;
	}
	public long getEndDateTimeMS() {
		return _endDateTimeMS;
	}
	public void setDone() {
		_isDone = true;
	}
	public void setNotDone() {
		_isDone = false;
	}
	public boolean isDone() {
		return _isDone;
	}

	public void clear() {
		_fileName="";
		_comparedRICs.clear();
		_isDone = false;
		_startDateTimeMS=0;
		_endDateTimeMS=0;
	}
}
