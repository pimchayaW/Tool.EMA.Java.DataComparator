///*|----------------------------------------------------------------------------------------------------
// *|            This source code is provided under the Apache 2.0 license      	--
// *|  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
// *|                See the project's LICENSE.md for details.                  					--
// *|           Copyright (C) 2019 Refinitiv. All rights reserved.            		--
///*|----------------------------------------------------------------------------------------------------

package com.lseg.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.refinitiv.ema.access.Msg;
import com.refinitiv.ema.access.OmmArray;
import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.ReqMsg;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;
import com.refinitiv.ema.access.Data;
import com.refinitiv.ema.access.DataType;
import com.refinitiv.ema.access.ElementList;
import com.refinitiv.ema.access.DataType.DataTypes;
import com.refinitiv.ema.rdm.EmaRdm;

import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.FieldEntry;
import com.refinitiv.ema.access.FieldList;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.OmmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AppClient implements OmmConsumerClient
{
	private static Logger Clientlogger = LoggerFactory.getLogger(AppClient.class);

	String _serviceName;
	
   Vector<DataProcessor> _dataProcessorVec;
   boolean _isStreaming;
   int _maxRefresh;
   int _numRefresh;
   int _index;
   ArrayDeque<UpdateMsgInfo> _snapshotMsgs;
   ArrayDeque<UpdateMsgInfo> _streamingMsgs;
   boolean _collectData;
   Object _lcollectData;
	public AppClient(String dataPath,String dateTime,String sn, int maxData, 
			TreeMap<String,String> ricsMap, Vector<Integer> checkFIDs, boolean isStreaming, int numSubRICs) {
		try {
			
			_serviceName = sn;
			_isStreaming = isStreaming;
			_maxRefresh = numSubRICs;
			_numRefresh = 0;
			_snapshotMsgs = new ArrayDeque<UpdateMsgInfo>();
			_streamingMsgs= new ArrayDeque<UpdateMsgInfo>();
			_dataProcessorVec = new Vector<DataProcessor>();
			_dataProcessorVec.add(new DataProcessor(sn, maxData, dataPath, dateTime, ricsMap,checkFIDs,isStreaming,numSubRICs));
			_index = 0;
			_collectData = false;
			_lcollectData = new Object();
			
		}catch(Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	public ArrayDeque<UpdateMsgInfo> getSnapshotMsgs() {
		return _snapshotMsgs;
	}
	private void setCollectData(boolean b) {
		synchronized(_lcollectData) {
			_collectData=b;
		}
	}
	public boolean didCollectData() {
		synchronized(_lcollectData) {
			return _collectData;
		}
	}
	public void collectData() {
		if(_isStreaming==false) {
			_dataProcessorVec.get(_index).work(_snapshotMsgs);
			_snapshotMsgs.clear();
			setCollectData(true);
		}
	}
	public boolean finishCollectData() {
		if(_isStreaming==false) {
			return (_dataProcessorVec.get(_index).isRunning()==false);
		} else
			return false;
	}
	//remove TBD
	public void collectValidRICs() {
		_dataProcessorVec.get(_index)._collectValidRICs = true;
	}
	synchronized public void onRefreshMsg(RefreshMsg refreshMsg, OmmConsumerEvent event)
	{
		Clientlogger.info("Subscribing " + (refreshMsg.hasName() ? refreshMsg.name() : "<not set>") + " to " + _serviceName + " is successful.\n");
		long recievedUpdateMS = Calendar.getInstance().getTimeInMillis();
		UpdateMsgInfo updateInfo = new UpdateMsgInfo(EmaFactory.createRefreshMsg(refreshMsg),recievedUpdateMS);
		if(_isStreaming==false) {
			_snapshotMsgs.add(updateInfo);
			++_numRefresh;
			if(_numRefresh==this._maxRefresh) {
				collectData();
			}
		}	
	}
	
	synchronized public void onUpdateMsg(UpdateMsg updateMsg, OmmConsumerEvent event) 
	{
		
		long recievedUpdateMS = Calendar.getInstance().getTimeInMillis();
		UpdateMsgInfo updateInfo = new UpdateMsgInfo(EmaFactory.createUpdateMsg(updateMsg),recievedUpdateMS);
		//TBD - change later
		_streamingMsgs.add(updateInfo);
	}

	public void onStatusMsg(StatusMsg statusMsg, OmmConsumerEvent event) 
	{
		if(statusMsg.state().statusText().contains("Stream closed for batch") ||
				statusMsg.state().statusText().contains("Login stream was closed."))
			return;
		Clientlogger.error("Subscribing " + (statusMsg.hasName() ? statusMsg.name() : "<not set>") + " failed. The error is " + statusMsg.state().statusText()+"\n");
	}
	
	public void onGenericMsg(GenericMsg genericMsg, OmmConsumerEvent consumerEvent){}
	public void onAckMsg(AckMsg ackMsg, OmmConsumerEvent consumerEvent){}
	public void onAllMsg(Msg msg, OmmConsumerEvent consumerEvent){}


	
}

class ConsumerInstance implements Runnable
{
	private AppClient _appClient;
	OmmConsumer _consumer;
	ReqMsg _reqMsg;
	public ExecutorService _executor;
	String _serviceName;
	long _sleepMS;
	Vector<Long> _handles;
	boolean _batchReq = false;
//	boolean _canClosedConsumer = false;
	private static Logger Conslogger = LoggerFactory.getLogger(ConsumerInstance.class);
	boolean _allItemsSubscribed = false;
	boolean _run = true;
	boolean _terminate = false;
	public ConsumerInstance(String host, String username, String serviceName,String dateTime,String dataPath, long sleepMS,
			int maxMsgs,String emaConf, TreeMap<String,String> ricsMap, Vector<Integer> checkFIDs, boolean isStreaming,int numSubRICs)
	{
		
		
		_appClient = new AppClient(dataPath,dateTime,serviceName,maxMsgs,ricsMap,checkFIDs, isStreaming, numSubRICs);
		
		_reqMsg = EmaFactory.createReqMsg();
		_serviceName = serviceName;
		_sleepMS = sleepMS;
		_handles = new Vector<Long>();
		_consumer = EmaFactory.createOmmConsumer(EmaFactory.createOmmConsumerConfig(emaConf).host(host).username(username));
		
		_executor = Executors.newSingleThreadExecutor();
		
		_executor.execute(this);
	
	}
	
	public AppClient getAppClient() {
		return _appClient;
	}
	public String getServiceName() {
		return _serviceName;
	}
	
	public void openItem(Set<String> items, boolean isStreaming)
	{
	
		_reqMsg.clear();
		
		if(_batchReq==true) {
			ElementList batch = EmaFactory.createElementList();
			OmmArray array = EmaFactory.createOmmArray();
			for(String aRIC:items) {
				array.add(EmaFactory.createOmmArrayEntry().ascii(aRIC));
			}
			batch.add(EmaFactory.createElementEntry().array(EmaRdm.ENAME_BATCH_ITEM_LIST, array));
			_consumer.registerClient(_reqMsg.serviceName(_serviceName).payload(batch).interestAfterRefresh(isStreaming), _appClient);
		} else {
			for(String aRIC:items) {
				long handle = _consumer.registerClient(_reqMsg.serviceName(_serviceName).name(aRIC).interestAfterRefresh(isStreaming), _appClient);
				if(isStreaming==true)
					_handles.add(handle);
			}
		}
		
	}
	public void closedAllItems()
	{
		
		for(Long handle:_handles) {
			 _consumer.unregister(handle);
		}
		Conslogger.info(String.valueOf(_handles.size()) + " streaming items have been closed.\n");
		stopClient();
		//_canCleanup=true;
	}
	private void stopClient() {
		synchronized(this) {
			_run=false;
		}
	}
	private boolean isRunning() {
		synchronized(this) {
			return _run;
		}
	}
	private void setTerminate() {
		synchronized(this) {
			_terminate=true;
		}
	}
	public boolean isTerminate() {
		synchronized(this) {
			return _terminate;
		}
	}
	public void run()
	{
		try
		{
			Conslogger.info("Waitting consuming data of service "+ this._serviceName + "\n");
			Thread.sleep(_sleepMS);
			while(isRunning() && _appClient.didCollectData()==false) {
				Thread.sleep(5*1000);
			}
			_executor.shutdown();
			_consumer.uninitialize();
			_executor.awaitTermination(5, TimeUnit.SECONDS);
			Conslogger.info("Done consuming data of service "+ this._serviceName + "\n");
			setTerminate();
		}
		catch (Exception excp)
		{
			Conslogger.error("Error:",excp);
		}	
	}
	
}

public class Consumer 
{
	private static Logger logger = LoggerFactory.getLogger(Consumer.class);
	//static StringBuffer comparatorCMD = new StringBuffer("");

	public static void main(String[] args)
	{
		
		try 
		{
			if(args.length!=1) { 
				System.out.println("usage: java com.lseg.tools.Consumer <application_properties_file>");
				System.exit(0);
			}
			String propFileName = args[0];
			Properties appProps = Common.readPropertiesFile(propFileName);	
			int numSubscribedRICs = Integer.parseInt(appProps.get("num.subscribed.rics").toString());
			TreeMap<String,String> otherFeed2Elektron = buildMapRicListFromFile(appProps.get("rics.map.file").toString(),numSubscribedRICs);
			int maxRICsPerBatch = Integer.parseInt(appProps.get("max.rics.per.batch").toString());
			long subscribedIntervalS = Long.parseLong(appProps.get("subscribed.interval.seconds").toString());
			long runTimeS = Long.parseLong(appProps.get("run.time.seconds").toString());
			int maxDataMsgs = Integer.parseInt(appProps.get("max.updates.messages").toString());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
			sdf.setTimeZone(TimeZone.getTimeZone("GMT+0"));
			String dateTime = sdf.format(Calendar.getInstance().getTime());
			String[] checkFieldIDsStr =  appProps.get("check.fieldIDs").toString().split(",");
			Vector<Integer> checkFIDs = new Vector<Integer>();
			for(String cfidstr:checkFieldIDsStr)
				checkFIDs.add(Integer.parseInt(cfidstr));
			boolean streamingReq = Boolean.valueOf(appProps.get("request.streaming").toString());
			int maxRefresh=-1;
			if(streamingReq==false) {
				maxRefresh = otherFeed2Elektron.size();
			}
			ConsumerInstance ElektronConsumer = new ConsumerInstance(appProps.get("Primary.host.port").toString(),appProps.get("Primary.user").toString(),appProps.get("Primary.service").toString(),dateTime,appProps.get("output.path").toString() ,
					runTimeS*1000,maxDataMsgs,appProps.get("emaj.config").toString(),null, checkFIDs,streamingReq,maxRefresh);
			ConsumerInstance FeedConsumer = new ConsumerInstance(appProps.get("Compared.host.port").toString(),appProps.get("Compared.user").toString(),appProps.get("Compared.service").toString(),dateTime, appProps.get("output.path").toString(), 
					runTimeS*1000,maxDataMsgs,appProps.get("emaj.config").toString(),otherFeed2Elektron, checkFIDs,streamingReq,maxRefresh);
		
			TreeSet<String> feedbatchRICs = new TreeSet<String>();
			TreeSet<String> elektronbatchRICs = new TreeSet<String>();
			int numRICs=0;
			int numRICPerBatch=0;
			int maxNumRICs = otherFeed2Elektron.size();
			for(String aRIC:otherFeed2Elektron.keySet()) {
				++numRICPerBatch;
				++numRICs;
				feedbatchRICs.add(aRIC);
				elektronbatchRICs.add(otherFeed2Elektron.get(aRIC));
				if(numRICPerBatch==maxRICsPerBatch || numRICs==maxNumRICs) {
					FeedConsumer.openItem(feedbatchRICs,streamingReq);
					ElektronConsumer.openItem(elektronbatchRICs,streamingReq);
					numRICPerBatch=0;
					feedbatchRICs.clear();
					elektronbatchRICs.clear();
					if(numRICs!=maxNumRICs)
						Thread.sleep(subscribedIntervalS*1000);
				} 
			}
			 logger.info("Running " + String.valueOf(runTimeS) + " seconds.\n");
			 Thread.sleep(runTimeS*1000);
			 ElektronConsumer.closedAllItems();
			 FeedConsumer.closedAllItems();
			 if(ElektronConsumer.getAppClient().didCollectData()==false) { 
				 ElektronConsumer.getAppClient().collectValidRICs();//TBD - remove later
				 ElektronConsumer.getAppClient().collectData();
			 }
			 if(FeedConsumer.getAppClient().didCollectData()==false) { 
				 FeedConsumer.getAppClient().collectData();
			 }
		     logger.info("Waitting ConsumerInstances exits and all data messages are kept in the files\n");
		     while(ElektronConsumer.isTerminate()==false || FeedConsumer.isTerminate()==false 
		    		 || ElektronConsumer.getAppClient().finishCollectData()==false 
		    		 || FeedConsumer.getAppClient().finishCollectData()==false ) {
		    	 Thread.sleep(2*1000);
		     }
		     logger.info("All threads stops, the application exits.\n");
		   
		}
		catch (Exception excp)
		{
			System.out.println(excp);
			excp.printStackTrace();
		}
		System.exit(0);
		     
	}

	public static TreeMap<String,String> buildMapRicListFromFile(String ricFileName, int maxCount)
    {
        RandomAccessFile dataFile = null;
        if(maxCount!=-1)
        	--maxCount;
        try
        {
            int i = 0;
            dataFile = new RandomAccessFile(ricFileName, "r");
            String line = dataFile.readLine();
          //  ArrayList<String> itemList = new ArrayList<String>();
            TreeMap<String,String> ricMap = new TreeMap<String,String>();
            //itemList.add(line); //add first item from ricFileName
            String[] feed2Elektron = line.split(",");
            ricMap.put(feed2Elektron[0],feed2Elektron[1]);
            while (line != null && (line = line.trim()).length() > 0)
            {
                line = dataFile.readLine();
                if ((maxCount == -1 || i < maxCount) && line != null
                        && (line = line.trim()).length() > 0)
                {
                	String[] otherFeed2Elektron = line.trim().split(",");
                    ricMap.put(otherFeed2Elektron[0].trim(),otherFeed2Elektron[1].trim());
                    i++;
                }
            }
            dataFile.close();
            dataFile = null;
            return ricMap;
        }
        catch (FileNotFoundException ex)
        {
            return null;
        }
        catch (IOException ex)
        {
        	logger.error("IO error processing " + ex.getMessage() + "\n" + "Exiting..." + "\n");
        	ex.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
