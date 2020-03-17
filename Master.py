from threading import Thread, RLock
from collections import defaultdict
import zmq
import socket
import time

def nested_dict(n, type):
    if n == 1:
        return defaultdict(type)
    else:
        return defaultdict(lambda: nested_dict(n-1, type))

masterHeadFinished = 0
masterDataFile = nested_dict(2, list) # masterDataFile = { ip1: { port1: [ file1, file2, ... ], port2: [...], ... }, ip2: {...} }
dataKeepersState = nested_dict(2, bool) # dataKeepersState = { ip1: { port1: True, port2: False, ... }, ip2: { port1: True, ... }, ... }
filesDictionary = nested_dict(1,list) # filesDictionary = { filename1: [ { ip1: [port1, port2, ...], ip2: [...], ... } , instanceCount], filename2: [...] }
# filesDictionary["filenameKey.mp4"][1] = instanceCount
# filesDictionary["filenameKey.mp4"][0]["tcp:127.0.0.1"] = [8000, 8001, 8002]
iAmAliveDict = nested_dict(1,int)
headDataKeepers = {}


def getIp():
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    s.connect(("8.8.8.8",80))
    return s.getsockname()[0]


def clientRequestHandler(message, syncLock):
    global masterDataFile
    global dataKeepersState
    syncLock.acquire()
    print(dataKeepersState)
    if message[0] == "upload":
        # Checks whether there is a free port (j) for each ip (i)
        for i in dataKeepersState:
            for j in dataKeepersState[i]:
                if(dataKeepersState[i][j]):
                    dataKeepersState[i][j] = False # Make Port Busy
                    syncLock.release()
                    return [i,j,message[1]]
    elif message[0] == "download":
        for i in masterDataFile:
            for j in masterDataFile[i]:
                for k in masterDataFile[i][j]:
                    if k == message[1] and dataKeepersState[i][j]:
                        dataKeepersState[i][j] = False # Make Port Busy
                        syncLock.release()
                        return [i,j,message[1]]
    syncLock.release()               
    return None


def masterClientConnection(clientSocket,syncLock):
    global masterDataFile
    global dataKeepersState
    # Sending/Receiving data from client
    # Wait for next request from client
    message = []
    try:
        message = clientSocket.recv_pyobj()
        print(message)
    except zmq.error.Again:
        return
    # TODO: Function made by friedPotato7 use messege[upload/download,filename.mp4] and return arr[ip,port#,path of file] replaced by 8000
    # port = ["tcp://localhost:",8000,"Alberto Mardegan - Selfie del futuro.mp4"]
    port = clientRequestHandler(message, masterDataFile, dataKeepersState,syncLock)
    clientSocket.send_pyobj(port)

def masterDatakeeperConnection(masterIndex,datakeeperSocket, numberOfProcessesPerDataKeeper, syncLock):
    global masterDataFile
    global dataKeepersState
    global filesDictionary
    global iAmAliveDict
    
    try:
        string = datakeeperSocket.recv_string()
        topic, messagedata ,ip ,NodeIndex , processesIndex  = string.split()
    except zmq.error.Again:
        return
    if topic=="1" and messagedata=="1" :
        iAmAliveDict[ip] += 1
        print("Master index "+ str(masterIndex )+" Node " +NodeIndex+" Process "+ processesIndex +" is Alive\n")
    #Master - datakeeper success message
    if topic=="1" and messagedata=="2" :
        port=NodeIndex
        fileName=processesIndex
        syncLock.acquire()
        print("On Master index "+ str(masterIndex )+" File with Name: " + fileName +" Has Successfully uploaded on Machine with ip: "+ ip+"\n" )
        addFile(ip,port,fileName,filesDictionary, numberOfProcessesPerDataKeeper)
        dataKeepersState[ip][port] = True
        for i in range(numberOfProcessesPerDataKeeper):
            masterDataFile[ip][str(8000+i)].append(messagedata)
        syncLock.release()
    
    if topic=="1" and messagedata=="3" :
        port = NodeIndex
        syncLock.acquire()
        dataKeepersState[ip][port] = True
        syncLock.release()
        

def addFile (ip,port,fileName, numberOfProcessesPerDataKeeper):
    global filesDictionary
    if(len(filesDictionary[fileName]) == 0):
        temp = nested_dict(1,list)
        filesDictionary[fileName].append(temp)
        filesDictionary[fileName].append(0)
    filesDictionary[fileName][1] += 1
    for i in range(numberOfProcessesPerDataKeeper):
        filesDictionary[fileName][0][ip].append(8000+i)


def initialzeClientMasterConnection(masterIndex,startingPortMasterClient):
    # Bind ports for clients
    clientPort=startingPortMasterClient+masterIndex
    context = zmq.Context()
    clientSocket = context.socket(zmq.REP)
    clientSocket.bind("tcp://172.30.249.130:%s" % clientPort)
    clientSocket.RCVTIMEO = 1
    return clientSocket


def initialzeDatakeeperMasterConnection(masterIndex,numberOfNodes_Datakeeper, numberOfProcessesPerDataKeeper, syncLock):
    global masterHeadFinished
    global masterDataFile
    global dataKeepersState
    global iAmAliveDict
    global headDataKeepers
    
    print("masterHeadFinished = ", masterHeadFinished)
    # Bind ports for datakeeper
    print("Master index = "+ str(masterIndex))
    headDataKeepers=[]
    if masterIndex == 0:
        context1 = zmq.Context()
        masterReceiver = context1.socket(zmq.PULL)
        masterReceiver.bind("tcp://172.30.249.130:%s" % str(17777)) # getIp()
        initializedDataKeepers = 0
        syncLock.acquire()
        
        while initializedDataKeepers < numberOfNodes_Datakeeper * numberOfProcessesPerDataKeeper:
            address = masterReceiver.recv_pyobj()
            masterDataFile["tcp://"+address["ip"]+":"][str(8000+address["nodeIndex"])] = []
            dataKeepersState["tcp://"+address["ip"]+":"][str(8000+address["nodeIndex"])]= True
            if address["head"]:
                iAmAliveDict[address["ip"]] = 0
                headDataKeepers.append("tcp://"+str(address["ip"])+":"+str(5556+address["nodeIndex"]))
            initializedDataKeepers += 1
        masterHeadFinished = 1
        syncLock.release()
    else:
        while masterHeadFinished == 0:
            pass
#            print("ana master rakam " + str(masterIndex) + " mestany papa yegeeb el shared memory")
    if masterIndex != 0:
        print("I'm master #" + str(masterIndex) + " with the following data:")
        print("headDataKeepers:", headDataKeepers)
    context = zmq.Context()
    datakeeperSocket = context.socket(zmq.SUB)
    for j in headDataKeepers:
        datakeeperSocket.connect(j)
    topicfilter = "1"
    datakeeperSocket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)
    datakeeperSocket.RCVTIMEO = 1
    return datakeeperSocket


def nReplicatesMasterDatakeeper (masterIndex):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    port=10000+masterIndex
    socket.bind("tcp://172.30.249.130:%s" %port)
    return socket

def makeNReplicates(syncLock,nrSocket,n):
    global filesDictionary
    global masterDataFile
    global dataKeepersState
    for file in filesDictionary:
        instance_count = filesDictionary[file][1] #get el instance count bta3 file 
        print(filesDictionary[file])
        if instance_count < n:
            for i in range(n-instance_count):
                print("ana gowa el for loop bta3et mahmoud")
                source_machine = getSourceMachine(file,filesDictionary,dataKeepersState,syncLock)
                if source_machine == False:
                    print ("All source Machines are busy failed to Make n Replicates")
                    return
                machine_to_copy_1 = selectMachineToCopyTo(masterDataFile,syncLock,dataKeepersState,file)
                if machine_to_copy_1 == False:
                    print ("All Machines_To_Copy are busy failed to Make n Replicates")
                    return
                NotifyMachineDataTransfer(source_machine, machine_to_copy_1,nrSocket)
            print("----------------------------------------------------------------------------------")
            print("--                            N Replicates Done  !!!                            --")
            print("----------------------------------------------------------------------------------")


def getSourceMachine(file,syncLock):
    global filesDictionary
    global dataKeepersState
    #getFreeMachine=False
    srcMachine=[]
    srcMachine.append(file)
    syncLock.acquire()
   # while getFreeMachine == False:
    for ip in filesDictionary[file][0]:
        for port in filesDictionary[file][0][ip]:
            if dataKeepersState[ip][port]:
                #getFreeMachine=True
                dataKeepersState[ip][port] = False
                syncLock.release()
                srcMachine.append(ip)
                srcMachine.append(port)
                print("Source Machine Found \n")
                return srcMachine
    syncLock.release()
    return False


def selectMachineToCopyTo(syncLock,fileName):
    global masterDataFile
    global dataKeepersState
    notFound=True
    #selectMachine=False
    #while selectMachine==False:
    syncLock.acquire()
    for i in masterDataFile:
            for j in masterDataFile[i]:
                notFound=True
                for k in masterDataFile[i][j]:
                    if k==fileName:
                        notFound=False
                        break
                if notFound==True and dataKeepersState[i][j]:
                    dataKeepersState[i][j] = False # Make Port Busy
                    syncLock.release()
                    #selectMachine=True
                    print("Machine to Copy Found \n")
                    return[i,j]
    syncLock.release()
    return False


def NotifyMachineDataTransfer(source_machine, machine_to_copy,nrSocket):
    msgToSrcMachine=["tcp://"+str(machine_to_copy[0])+":"+machine_to_copy[1],"Alberto Mardegan - Selfie del futuro.mp4","source_machine",str(source_machine[1]),str(source_machine[2])]
    topic = "1"
    nrSocket.send("%d %d" % (topic, msgToSrcMachine)) #send to source machine ip and port of "machine_to_copy" and filename  and variable to know it is source_machine


def masterTracker(masterIndex,numberOfNodes_Datakeeper, numberOfProcessesPerDataKeeper, startingPortMasterClient,syncLock,replicatesCount):
    
    global masterHeadFinished
    global masterDataFile
    global dataKeepersState
    global filesDictionary
    global iAmAliveDict
    global headDataKeepers
    timerCounter = 0
    
    clientSocket = initialzeClientMasterConnection(masterIndex,startingPortMasterClient)
    datakeeperSocket = initialzeDatakeeperMasterConnection(masterIndex,numberOfNodes_Datakeeper, numberOfProcessesPerDataKeeper, syncLock)
    #nReplicates Master Datakeeper Connection
    nrSocket = nReplicatesMasterDatakeeper(masterIndex)
    startTime = time.time()
    while True:
#        print("Master rakam " + str(masterIndex) + " by2olak ana mawgood")
        #Connecting with client
        masterClientConnection(clientSocket, syncLock)
        # Connecting with data 
        masterDatakeeperConnection(masterIndex,datakeeperSocket, numberOfProcessesPerDataKeeper, syncLock)
        if time.time() - startTime > 1:
            timerCounter += 1
            print("iAmAliveDict of master #" + str(masterIndex), iAmAliveDict)
            syncLock.acquire()
            willDel=[]
            for ip in iAmAliveDict:
                if iAmAliveDict[ip]+1 < timerCounter:
                    if dataKeepersState["tcp://"+ip+":"][str(8000)]==True :                         
                        print("master index " +str(masterIndex))
                        print("Datakeeper on ip: " + ip + " is dead, removing it from Master shared memory...")
                        del masterDataFile["tcp://"+ip+":"]
                        del dataKeepersState["tcp://"+ip+":"]
                        willDel.append(ip)
                        for i in filesDictionary:
                            filesDictionary[i][1] -= 1
                            del filesDictionary[i][0][ip]
                    else:
                        iAmAliveDict[ip] = timerCounter
                        
            for i in willDel:
                del iAmAliveDict[i]
            syncLock.release()
            startTime = time.time()
        makeNReplicates(syncLock,nrSocket,replicatesCount) 

def main():
    numberOfthreadssOfMaster=5 #number of processes  multi-process(MasterTracker)
    numberOfprocessesOfNodes=3 #number of processes  multi-process(data keeper)
    numberOfNodes=1 #number of nodes of data keeper
    startingPortMasterClient=7000 #first port between client/master
    startingPortDatakeeperClient=8000 #first port between client/datakeeper
    masterIp="tcp://172.30.249.130:" #master ip
    replicatesCount=2 # count of replicates
    threads=[]
    
    syncLock = RLock()

    for k in range(numberOfthreadssOfMaster):
        t= Thread(target=masterTracker,args=(k,numberOfNodes, numberOfprocessesOfNodes,startingPortMasterClient, syncLock,replicatesCount,)) 
        threads.append(t)

    for j in threads:
        j.start()
        
    for j in threads:
        j.join()
    print("Done!")
    while(True):
        []
        
main()    