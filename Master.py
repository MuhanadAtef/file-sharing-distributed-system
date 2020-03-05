import zmq

def masterTracker(masterIndex,numberOfNodes_Datakeeper):
    print("Master index = "+ str(masterIndex))
    if masterIndex==0: 
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
    
    
        datakeeper_StartPort = 5556
        portArr=[]
        for i in range(numberOfNodes_Datakeeper):     
            t=datakeeper_StartPort+i
            portArr.append(t)
        for j in portArr:
            socket.connect ("tcp://127.0.0.1:%s" %  str(j))
        topicfilter = "1"
        socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)

    while True:
        if masterIndex==0:
            string = socket.recv_string()
            topic, messagedata , NodeIndex , processesIndex  = string.split()
            if topic=="1" and messagedata=="1" :
                print("Master index "+ str(masterIndex )+" Node " +NodeIndex+" Process "+ processesIndex +" is Alive\n")
            