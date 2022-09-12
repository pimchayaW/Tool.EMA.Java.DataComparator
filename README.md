# Tool.EMA.Java.DataComparator
Direct Feeds data verification tool project

# Overview
Refer to Docs/DataComparator.pptx

# Prerequisites:
1. Java 1.8
2. RTSDK Java 2.0.6 or higher. The packages are available at https://developers.refinitiv.com/en/api-catalog/refinitiv-real-time-opnsrc/rt-sdk-java/download
3. Server(s) which provides RSSL connection of both feeds
4. List of items of each feed separated by ",". The example is 4263__EDF_TSF_ELEKTRON_REPLABLINUX3.txt


# How to run
1. Download release folder to put to your linux machine.
2. Modify appConfig.properties to consume field ids and compare list of items from your server(s).
3. Modify keepData.sh and compareData.sh to point to your JAVA and RTSDK Java
4. Run the tool in background to consume data of both feeds using the following command line:
    nohup /home/rep01/dataComparator/keepData.sh appConfig.properties &
5. Run the tool in background to compare data of both feeds using the following command line:
    nohup /home/rep01/dataComparator/compareData.sh appConfig.properties &
