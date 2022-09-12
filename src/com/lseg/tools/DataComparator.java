package com.lseg.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataComparator {
	private static Logger DataComparatorlogger = LoggerFactory.getLogger(DataComparator.class);
	String _rtService;
	String _feedService;
	String _rtDataFile;
	String _feedDataFile;
	//String _compareFile;
//	String _dateTime;
	//String _index;
//	static String _pendingDataFile="";
//	final static String _pendingFilePrefix = "PendingComapre_";
	String _outputPath;
	Properties _appProps;
	int _maxMsgBatch;
	int _numCheckedFids;
	TreeMap<String,UpdateInfo> rtUpdates;
	TreeMap<String,UpdateInfo> feedUpdates;
	TreeMap<String,UpdateInfo> notFoundUpdates;
	HashSet<String> _rtDataRICs;
	HashSet<String> _feedDataRICs;
	int _compareIntervalM;
//	boolean _endRTDataFile;
	//boolean _endFDataFile;
	//boolean _endFile;
	String _idFile;
	/*public DataComparator(String rtDataFile, String feedDataFile, String propFile) {
		_rtDataFile = rtDataFile;
		_feedDataFile = feedDataFile;
		try {
			_appProps = Common.readPropertiesFile(propFile);	
			_outputPath = _appProps.get("output.path").toString();
		}catch(Exception e) {
			DataComparatorlogger.error("Intializing DataComparator failed. " + "\n" + e.getMessage()+"\n");
			e.printStackTrace();
			System.exit(1);
		}
		//1_20220829T053919_ELEKTRON_RT.txt
		String[] tmp = rtDataFile.split("_");
		_index = tmp[0].replace(_outputPath, "");
		_dateTime = tmp[1];
		DataComparatorlogger.info("index="+ _index + "\n");
		DataComparatorlogger.info("dateTime="+ _dateTime + "\n");
		if(_pendingDataFile.isEmpty()==true) {
			_pendingDataFile = _pendingFilePrefix + _dateTime + ".csv";
		}	
		_compareFile = "Comparing_" + _index + "_" + _dateTime + ".csv";
		_maxMsgBatch = Integer.parseInt(_appProps.get("comparator.max.msgs.abatch").toString());
		String[] checkFieldIDsStr =  _appProps.get("check.fieldIDs").toString().split(",");
		_numCheckedFids = checkFieldIDsStr.length;
		rtUpdates = new TreeMap<String,UpdateInfo>();
		feedUpdates	= new TreeMap<String,UpdateInfo>();
		notFoundUpdates= new TreeMap<String,UpdateInfo>();
		_endRTDataFile = false;
		_endFDataFile = false;
		_endFile = false;
		
	}*/
	private String[] getDirectoryPath(String path) {
		 File directoryPath = new File(path);
	      //List of all files and directories
	      String contents[] = directoryPath.list();
	      return contents;
	}
	public DataComparator(String propFile) {
		
		try {
			_appProps = Common.readPropertiesFile(propFile);	
			_outputPath = _appProps.get("output.path").toString();
			_rtService =  _appProps.get("Primary.service").toString();
			_feedService =  _appProps.get("Compared.service").toString();
			_maxMsgBatch = Integer.parseInt(_appProps.get("comparator.max.msgs.abatch").toString());
			_compareIntervalM=Integer.parseInt(_appProps.get("comparator.interval.minutes").toString());
			String[] checkFieldIDsStr =  _appProps.get("check.fieldIDs").toString().split(",");
			_numCheckedFids = checkFieldIDsStr.length;
			rtUpdates = new TreeMap<String,UpdateInfo>();
			feedUpdates	= new TreeMap<String,UpdateInfo>();
			notFoundUpdates= new TreeMap<String,UpdateInfo>();
			_rtDataRICs = new HashSet<String>();
			_feedDataRICs=new HashSet<String>();
			/*_endRTDataFile = false;
			_endFDataFile = false;
			_endFile = false;*/
		}catch(Exception e) {
			DataComparatorlogger.error("Intializing DataComparator failed. " + "\n" + e.getMessage()+"\n");
			e.printStackTrace();
			System.exit(1);
		}
		
		//1_20220829T053919_ELEKTRON_RT.txt
		
		/*String[] tmp = rtDataFile.split("_");
		_index = tmp[0].replace(_outputPath, "");
		_dateTime = tmp[1];
		DataComparatorlogger.info("index="+ _index + "\n");
		DataComparatorlogger.info("dateTime="+ _dateTime + "\n");
		if(_pendingDataFile.isEmpty()==true) {
			_pendingDataFile = _pendingFilePrefix + _dateTime + ".csv";
		}	*/
		/* File directoryPath = new File(output);
	      String contents[] = directoryPath.list();
		_compareFile = "Comparing_" + _index + "_" + _dateTime + ".csv";
		_maxMsgBatch = Integer.parseInt(_appProps.get("comparator.max.msgs.abatch").toString());
		String[] checkFieldIDsStr =  _appProps.get("check.fieldIDs").toString().split(",");
		_numCheckedFids = checkFieldIDsStr.length;
		rtUpdates = new TreeMap<String,UpdateInfo>();
		feedUpdates	= new TreeMap<String,UpdateInfo>();
		notFoundUpdates= new TreeMap<String,UpdateInfo>();
		_endRTDataFile = false;
		_endFDataFile = false;
		_endFile = false;*/
		
	}
	/*public void loadDataMsgs(String dataFileName)  {
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(dataFileName);
			br = new BufferedReader(fr);
			String line;
			long numLine=0;
			DataComparatorlogger.info("loading " + dataFileName + "\n");
			//System.out.println("loading " + dataFileName + "\n");
			while ((line = br.readLine()) != null) {
				++numLine;
			}
		}catch (IOException e) {

			e.printStackTrace();

		} finally {
			
			try {

				if (br != null) {
					br.close();
					br = null;
				}

				if (fr != null) {
					fr.close();
					fr=null;
				}
			}catch (IOException ex) {
				DataComparatorlogger.error(ex.getMessage()+ "\n");
				ex.printStackTrace();
			}

		}
				
	}*/
	public TreeMap<String,UpdateInfo> loadUpdaresFromDataFile(String dataFile,boolean isRealTime) {
		TreeMap<String,UpdateInfo> updatesMap = new TreeMap<String,UpdateInfo>();
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(dataFile);
			br = new BufferedReader(fr);
			String line="";
			
			DataComparatorlogger.info("loading " + dataFile + "\n");
			
			StringBuffer dtSB = new StringBuffer("");
			StringBuffer ricSB = new StringBuffer("");
			StringBuffer snSB = new StringBuffer("");
			UpdateInfo anUpdate;
			int noLine=0;
			while ((line = br.readLine()) != null ) {
				++noLine;
			    if(noLine % 2 == 1)//info
				{
					
					String[] info = line.split(";");//20220830T063411.017;2914.T;2
					if(info.length==3)
					{
						dtSB.append(info[0]);
						ricSB.append(info[1]);
						snSB.append(info[2]);
						if(isRealTime) 
							_rtDataRICs.add(ricSB.toString());
						else
							_feedDataRICs.add(ricSB.toString());
						
					} else {
						DataComparatorlogger.warn("Cannot parse line:" + String.valueOf(noLine) + " in " + dataFile + "\n");
					}
				} else if(noLine % 2 == 0){
				
					if(line.startsWith("<")) {//no required field
						anUpdate = new UpdateInfo(dtSB.toString(),ricSB.toString(),Integer.parseInt(snSB.toString()));
						updatesMap.put(anUpdate.getID(), new UpdateInfo(anUpdate));
						//clear for the next update
					/*	dtSB.setLength(0);
						ricSB.setLength(0);
						snSB.setLength(0);*/	
					}
					//BID=2355.5;ASK=2356.0;BID_TIME=01:19:56:000:000:000;
					//BID=2355.5;
					else {
						String[] fids = line.split(";");
						int numfids = (fids.length<this._numCheckedFids)? fids.length : this._numCheckedFids;
						anUpdate = new UpdateInfo(dtSB.toString(),ricSB.toString(),Integer.parseInt(snSB.toString()));
						for(int i=0; i< numfids; ++i) {
							String[] fidValue = fids[i].split("=");
							if(fidValue.length==2) {
								anUpdate.setAField(fidValue[0], fidValue[1]);
							}
						}
						updatesMap.put(anUpdate.getID(), new UpdateInfo(anUpdate));
					}
					//clear for the next update
					dtSB.setLength(0);
					ricSB.setLength(0);
					snSB.setLength(0);	
				}
			}
		}catch (IOException e) {

			DataComparatorlogger.error("Reading " + dataFile  + " failed \n" + "Error is " + e.getMessage()+ "\n");
			e.printStackTrace();
		} finally {
			
			try {

				if (br != null) {
					br.close();
					br = null;
				}

				if (fr != null) {
					fr.close();
					fr=null;
				}
				
			}catch (IOException ex) {
				DataComparatorlogger.error("Reading " + dataFile  + " failed \n" + "Error is " + ex.getMessage()+ "\n");
				ex.printStackTrace();
			}
			return new TreeMap<String,UpdateInfo>(updatesMap);
		}
				
	}

	private boolean findComparedFiles(String _outputPath) {
		DataComparatorlogger.info("Searching data files in " + _outputPath + "\n");
		String[] dataFiles = getDirectoryPath(_outputPath);
		_rtDataFile="";
		_feedDataFile="";
		for(String aFile:dataFiles) {
			if(aFile.startsWith(_rtService)) {
				_rtDataFile = aFile;
				String tmp = _rtDataFile;
				_idFile = tmp.replace(_rtService, "");
				//System.out.println(_idFile);
				for(String afeedFile:dataFiles) {
					//System.out.println("c-" + afeedFile);
					if(afeedFile.endsWith(_idFile) && afeedFile.equals(_rtDataFile)==false) {
						_feedDataFile = afeedFile;
						return true;
					}
				}
			}
		}
		return(_feedDataFile.isEmpty()==false);
	}
	public void work() {
		DataComparatorlogger.info("Start comparing...\n");
		while(findComparedFiles(_outputPath)==true) {
			DataComparatorlogger.info("the data files are found.\n");
			String fullrtData = _outputPath+_rtDataFile;
			String fullfeedData = _outputPath+_feedDataFile;
			String fullrtCompareData = _outputPath+"Comparing_"+_rtDataFile;
			String fullfeedCompareData = _outputPath+"Comparing_"+_feedDataFile;
			String outputFile = _outputPath + "Comparing_" + _idFile;
			renameFile(fullrtData,fullrtCompareData);
			renameFile(fullfeedData,fullfeedCompareData);
			DataComparatorlogger.info("Comparing(max=" +this._maxMsgBatch+")" + fullrtCompareData + " and " + fullfeedCompareData +  "\n");
			compareData(fullrtCompareData,fullfeedCompareData,outputFile,this._maxMsgBatch );
		}
	}
	public void compareData(String rtDataFile, String fDataFile, String outputFile,int maxCompareMsgs) {
		FileWriter fw = null;
		int sameMsgs = 0;
		int diffMsgs = 0;
		try {
			
			
			rtUpdates = loadUpdaresFromDataFile(rtDataFile, true);
			feedUpdates= loadUpdaresFromDataFile(fDataFile, false);
			StringBuffer notUpdateRICs = new StringBuffer("");
			for(String aRIC:_rtDataRICs) {
				if(_feedDataRICs.contains(aRIC)==false)
					notUpdateRICs.append(aRIC).append(",");
			}
			if(notUpdateRICs.length()>0) {
				DataComparatorlogger.info(this._feedService + " did not update " + notUpdateRICs.toString()+ "\n");
			}
			boolean same = true;
			
			fw = new FileWriter(outputFile);
			fw.write("RIC,SeqNo,Difference,missed fields,diff_value(RealTime|Feed),RealTime Received dateTime,Feed Received dateTime\n");
			fw.flush();
			DataComparatorlogger.info("Comparing " +rtDataFile + " and " + fDataFile + "\n");
			DataComparatorlogger.info(rtUpdates.size()+ " real-time data messages and " + feedUpdates.size() + " feed data messages\n");
			for(String rUpdateKey:rtUpdates.keySet()) {
				if(feedUpdates.containsKey(rUpdateKey)==false) {//missed message
					notFoundUpdates.put(rUpdateKey, new UpdateInfo(rtUpdates.get(rUpdateKey)));
					fw.write(rtUpdates.get(rUpdateKey).getRIC() + "," 
							+ String.valueOf(rtUpdates.get(rUpdateKey).getSeqNum()) + "," 
							+ "miss message,"
							+  rtUpdates.get(rUpdateKey).getAllValues() + ","
							+ ","
							+ rtUpdates.get(rUpdateKey).getDateTime() + ","
							+"\n");
					fw.flush();
					same = false;
				}
				else {
					TreeMap<String,String> rfields = 	 rtUpdates.get(rUpdateKey).getFields();
					TreeMap<String,String> ffields = feedUpdates.get(rUpdateKey).getFields();
					StringBuffer missedFids = new StringBuffer("");
					StringBuffer diffFids = new StringBuffer("");
					//TBD?
					for(String rfid:rfields.keySet()) { 
						if(ffields.containsKey(rfid)==false) {//missed fields
							missedFids.append(rfid + "=" + rfields.get(rfid) + ";");
						} else { //checkValue
							if(rfields.get(rfid).equals(ffields.get(rfid))==false) {//diff value
								diffFids.append(rfid + "=" + rfields.get(rfid) + "|" + ffields.get(rfid) + ";");
							}
						}
					}
					if(missedFids.length()>0) {
						fw.write(rtUpdates.get(rUpdateKey).getRIC() + "," 
							+ String.valueOf(rtUpdates.get(rUpdateKey).getSeqNum()) + "," 
							+ "miss fields,"
							+ missedFids.toString() + ","
							+ ","
							+ rtUpdates.get(rUpdateKey).getDateTime() + ","
							+ feedUpdates.get(rUpdateKey).getDateTime()
							+"\n");	
						fw.flush();
						same = false;
					}
					if(diffFids.length()>0) {
						fw.write(rtUpdates.get(rUpdateKey).getRIC() + "," 
								+ String.valueOf(rtUpdates.get(rUpdateKey).getSeqNum()) + "," 
								+ "diff values,"
								+ "," 
								+ diffFids.toString() + ","
								+ rtUpdates.get(rUpdateKey).getDateTime() + ","
								+ feedUpdates.get(rUpdateKey).getDateTime() 
								+ "\n");	
						fw.flush();
						same = false;
					}
				}
				if(same==true) {
					++sameMsgs;
				} else 
					++diffMsgs;
				same=true;
				
			}
			if(sameMsgs>0)
				DataComparatorlogger.info(String.valueOf(sameMsgs) + " messages are same.\n"); 
			if(diffMsgs>0)
				DataComparatorlogger.info(String.valueOf(diffMsgs) + " messages are difference.\n");
			//Thread.sleep(120*1000);
			
		}catch (Exception ex) {
			DataComparatorlogger.error(ex.getMessage()+ "\n");

		}finally {
			try {
				rtUpdates.clear();
				feedUpdates.clear();
				notFoundUpdates.clear();
				_rtDataRICs.clear();
				_feedDataRICs.clear();
				if(fw != null) {
					fw.close();
					fw=null;
				}
				String comparingRTFile = _outputPath + "Comparing_" + _rtDataFile;
				String comparedRTFile = _outputPath + "Compared_" + _rtDataFile;
				renameFile(comparingRTFile,comparedRTFile);
				String comparingFeedFile = _outputPath + "Comparing_" + _feedDataFile;
				String comaparedFeedFile = _outputPath + "Compared_" + _feedDataFile;
				renameFile(comparingFeedFile,comaparedFeedFile);
				if(diffMsgs>0) {
					_idFile = _idFile.replace("txt", "csv");
					String newFile =_outputPath + "Compared_" + _idFile;
					if(renameFile(outputFile,newFile)) {
						DataComparatorlogger.info("Done Comparing. Please see the differences in " + newFile + "\n");
					} else {
				    	 DataComparatorlogger.info("Done Comparing. Please see the differences in " +outputFile + "\n");
					}
				} else {
						DataComparatorlogger.info("Done Comparing. The feed sent the same messages as Real-time feed.\n");
						File comparedFile = new File(outputFile);
						comparedFile.delete();
				}
				
			} catch (IOException ex) {
				DataComparatorlogger.error(ex.getMessage()+ "\n");
				ex.printStackTrace();
			}
		}
	}
	private boolean renameFile(String oldFileName, String newFileName) {
		File oldfile = new File(oldFileName);
		File newfile = new File(newFileName);
		return oldfile.renameTo(newfile);
	}
	public static void main(String[] args) {
		if(args.length!=1) { 
			System.out.println("usage: java com.lseg.tools.DataComparator <application_properties_file>");
			System.exit(0);
		}
		DataComparator compare=new DataComparator(args[0]);
		while(true) {
			try {
				Thread.sleep(compare._compareIntervalM*1000*60);
				compare.work();
			}catch(Exception e) {
				DataComparatorlogger.error(e.getMessage()+ "\n");
			}
		}
	
	}
}
