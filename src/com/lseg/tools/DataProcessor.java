package com.lseg.tools;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.UpdateMsg;

class UpdateMsgInfo {
	UpdateMsg _update;
	long _receivedTimeMS;
	RefreshMsg _refresh;
	boolean _isUpdate=true;
	
	public UpdateMsgInfo(UpdateMsg update, long receivedTimeMS) {
		_update = update;
		_receivedTimeMS = receivedTimeMS;
		_isUpdate = true;
	}
	public UpdateMsgInfo(RefreshMsg refresh, long receivedTimeMS) {
		_refresh = refresh;
		_receivedTimeMS = receivedTimeMS;
		_isUpdate=false;
	}
	
}
public class DataProcessor implements Runnable {
	 ArrayDeque<UpdateMsgInfo> _waitPendingDataMessages=new  ArrayDeque<UpdateMsgInfo>();
	/// ArrayDeque<UpdateMsgInfo> _workingDataMessages=new  ArrayDeque<UpdateMsgInfo>();
//	ArrayDeque<DataInfo> _waitComparedDataFile=new  ArrayDeque<DataInfo>();
	private static Logger dataProcessorlogger = LoggerFactory.getLogger(DataProcessor.class);
	public ExecutorService _executor;
	private Object _lrun;
	FileWriter _fw; 
	String _fileName;
	String _serviceName;
	int _fileIndex=1;
	int _maxData=0;
	int _numData=0;
	String _dataPath;
	StringBuffer _dataFileName;
	String _dateTime;
	TreeMap<String,String> _ricsMap;
	Vector<Integer> _checkFIDs;
	DataInfo _dataInfoFile;
    AppClient _notifiedAppClient;
	SimpleDateFormat _sdf; 
	long _recievedUpdateMS;
	boolean _run;
	boolean _processAllUpdates;
	boolean _writeUpdate=false;
	boolean _writeRefresh=false;
	boolean _isStreaming=false;
	int _maxRefresh;
	int _numRefresh=0;
	String _state = "Collecting";
	//TBD -remove later
	String _validRICs=null;
	FileWriter _rfw;
	public boolean _collectValidRICs=false;
	public DataProcessor(String serviceName, int maxData, String dataPath, String dateTime, 
			TreeMap<String,String> ricsMap, Vector<Integer> checkFIDs,boolean isStreaming, int maxRefresh) {
		_isStreaming = isStreaming;
		_serviceName = serviceName;
		//_notifiedAppClient=null;
		_maxData = maxData;
		_dataPath = dataPath;
		_dateTime = dateTime;
		//_dataFileName = new StringBuffer(_dataPath +String.valueOf(_fileIndex) + "_" + _dateTime + "_" + _serviceName + ".txt");
		_dataFileName = new StringBuffer(_dataPath + _state + "_"+ _serviceName + "_" + _dateTime + "_" + String.valueOf(_fileIndex)+ ".txt");
		_dataInfoFile = new DataInfo(_dataFileName.toString());
		_maxData = maxData;
		if(ricsMap != null) {
			_ricsMap =  new TreeMap<String,String>(ricsMap);
		} else
			_ricsMap = null;
		_checkFIDs = new Vector<Integer>(checkFIDs);
		_sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS");
		_sdf.setTimeZone(TimeZone.getTimeZone("GMT+0"));
		//_recievedUpdateMS=0;
		_run = false;
		_processAllUpdates = false;
		_maxRefresh = maxRefresh;
		_lrun = new Object();
		
		
	}
	public void work(ArrayDeque<UpdateMsgInfo> msgs) {
		if(_run==true)
			return;
		try {
			_run = true;
			_waitPendingDataMessages = new ArrayDeque<UpdateMsgInfo>(msgs);
			_fw = new FileWriter(_dataFileName.toString());
			
			//TBD -remove later
			try
	        { 
				if(_collectValidRICs==true) {
					_validRICs = System.getProperty("valid.rics"); 
					_rfw = new FileWriter(_validRICs);
				} else {
					_validRICs = null;
		        	_rfw = null;
				}
	        }
	        catch (Exception e)
	        { 
	        	_validRICs = null;
	        	_collectValidRICs = false;
	        	_rfw = null;
	        }
			if(_validRICs==null) {
				_collectValidRICs = false;
	        	_rfw = null;
			}
			//TBD
			_executor = Executors.newSingleThreadExecutor();
			_executor.execute(this);
		}catch(Exception e) {
			dataProcessorlogger.error("Error:");
			e.printStackTrace();
			System.exit(1);
		}

	}
	synchronized public void setStop() { 
		synchronized(_lrun) {
			_run = false;
		}
	}
	public boolean isRunning() {
		synchronized(_lrun) {
			return _run;
		}
	}
	
	 private void processUpdateMsg(UpdateMsg updateMsg, long receivedTimeMS) {
			if (DataType.DataTypes.FIELD_LIST == updateMsg.payload().dataType()) {
				String ric = "<not set>";
				if(updateMsg.hasName()) {
					_recievedUpdateMS = receivedTimeMS;
					//_recievedUpdateMS = Calendar.getInstance().getTimeInMillis();
					if(_dataInfoFile.hasStartDateTimeMS()==false)
						_dataInfoFile.setStartDateTimeMS(_recievedUpdateMS);
					if(_ricsMap!=null) {
						ric = _ricsMap.get(updateMsg.name());
					} else {
						ric = updateMsg.name();
					}
					_dataInfoFile.foundRIC(ric);
					
				} 
				//System.out.println("ric="+ ric);
				if(ric.equals("<not set>")) {
					return;
				}
				try {
					if(_fw==null) {
						return;
					}
					_fw.write(_sdf.format(new Date(receivedTimeMS))
							+";" + ric
							+";" + String.valueOf(updateMsg.seqNum())
							+"\n");
					_fw.flush(); 
					String dataStr = decode(updateMsg.payload().fieldList());	
					if(dataStr.length()>0) 
						_fw.write(dataStr);
					 else
						_fw.write("<The fields are not found>\n");
					_fw.flush(); 
					++_numData;
					if(_isStreaming==true && _numData==_maxData) {
						_dataInfoFile.setEndDateTimeMS(_recievedUpdateMS);
						_fw.close();
						_fw = null;
						dataProcessorlogger.info("data messages has been written in " + _dataFileName.toString() +"\n");	
						++_fileIndex;
						_numData = 0;
						_dataFileName.setLength(0);
						_dataFileName.append(_dataPath +_serviceName + "_" + _dateTime + "_" + String.valueOf(_fileIndex)+ ".txt");
						_dataInfoFile.clear();
						_dataInfoFile.setFileName(_dataFileName.toString());
						_fw = new FileWriter(_dataFileName.toString());
					}
				} catch(Exception e) {
					dataProcessorlogger.error("Writing " +ric + " failed.\n" + "Error:", e);
				} 
			}
			
	 }
	 
	 private void processRefreshMsg(RefreshMsg refreshMsg, long receivedTimeMS) {
			
			if (DataType.DataTypes.FIELD_LIST == refreshMsg.payload().dataType()) {
				String ric = "<not set>";
				if(refreshMsg.hasName()) {
					_recievedUpdateMS = receivedTimeMS;
					//_recievedUpdateMS = Calendar.getInstance().getTimeInMillis();
					if(_dataInfoFile.hasStartDateTimeMS()==false)
						_dataInfoFile.setStartDateTimeMS(_recievedUpdateMS);
					if(_ricsMap!=null) {
						ric = _ricsMap.get(refreshMsg.name());
					} else {
						ric = refreshMsg.name();
					}
					_dataInfoFile.foundRIC(ric);
					
				} 
				//System.out.println("ric="+ ric);
				if(ric.equals("<not set>"))
					return ;
				try {
					if(_fw==null) {
						
						return ;
					}
					_fw.write(_sdf.format(new Date(receivedTimeMS))
							+";" + ric
							+";" + String.valueOf(refreshMsg.seqNum())
							+"\n");
					_fw.flush(); 
					
					String dataStr = decode(refreshMsg.payload().fieldList());	
				
					if(dataStr.length()>0) 
						_fw.write(dataStr);
					 else
						_fw.write("<The fields are not found>\n");
					_fw.flush(); 
					
					//TBD -remove later
					if(_collectValidRICs==true) {
						_rfw.write(ric+"\n");
						_rfw.flush();
					}
				} catch(Exception e) {
					dataProcessorlogger.error("Writing " +ric + " failed.\n" + "Error:", e);
				} 
			}
			
			
	 }
			String decode(FieldList fieldList) throws Exception
			{
				StringBuffer fieldStr=new StringBuffer("");
				Iterator<FieldEntry> iter = fieldList.iterator();
				FieldEntry fieldEntry;
				while (iter.hasNext())
				{
					fieldEntry = iter.next();
					for(int cfid:_checkFIDs) {
					//	Clientlogger.info("Checking " + String.valueOf(cfid));
						if(fieldEntry.fieldId()==cfid) {
							//_fw.write(fieldEntry.name() + "=" + fieldEntry.load().toString()+";");
							fieldStr.append(fieldEntry.name() + "=" + fieldEntry.load().toString()+";");
							//writeData = true;
							break;
						}
					}
					
				}
				if(fieldStr.length()>0)
					fieldStr.append("\n");
				return fieldStr.toString();
			}
	
	public void run() {
		dataProcessorlogger.info("Start writting " + String.valueOf(_waitPendingDataMessages.size()) + " messages of service "+ _serviceName + "\n");
		UpdateMsgInfo updateInfo=null;
		try {
			while( _waitPendingDataMessages.isEmpty()==false) {
				updateInfo =  _waitPendingDataMessages.poll();
					if(updateInfo._isUpdate==true) {
						//_writeUpdate=true;
						processUpdateMsg(updateInfo._update,updateInfo._receivedTimeMS);
						//_writeUpdate=false;
					}
					else {
						//_writeRefresh = true;
						processRefreshMsg(updateInfo._refresh,updateInfo._receivedTimeMS);
						//_writeRefresh = false;
					}
			}
			//all responses of snapshot
			_dataInfoFile.setEndDateTimeMS(updateInfo._receivedTimeMS);
			_fw.close();
			_fw = null;
			//rename
			File oldfile = new File(_dataFileName.toString());
			_dataFileName.setLength(0);
			_dataFileName.append( _dataPath +_serviceName + "_" + _dateTime + "_" + String.valueOf(_fileIndex)+ ".txt");
			File newfile = new File(_dataFileName.toString());
			oldfile.renameTo(newfile);
			dataProcessorlogger.info("data messages has been written in " + _dataFileName.toString() +"\n");
			//TBD -remove later
			if(_rfw!=null) {
				_rfw.close();
				_rfw=null;
			}
			setStop();
			_executor.shutdown();
			_executor.awaitTermination(5, TimeUnit.SECONDS);
			dataProcessorlogger.info("dataProcessor of service " + _serviceName + " exits.\n");
		}catch (Exception excp)
		{
			dataProcessorlogger.error("Error:",excp);
		} finally {
			
		}
	}
	
}
