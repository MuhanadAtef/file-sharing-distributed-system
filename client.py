import zmq
import random
import time

def client(masterIp,startingPortMasterClient,numberOfprocessesOfMaster,commands):
    context = zmq.Context() 
    masterSocket = context.socket(zmq.REQ) # Master-client connection
    datakeeperSocket = context.socket(zmq.PAIR) # Datakeeper-client connection
    ls=[] # Holding ports of the master to be connected to client
    # Generate random connection to master processes
    for i in range(numberOfprocessesOfMaster):
        ls.append(startingPortMasterClient+i)
    ran=random.sample(ls,k=numberOfprocessesOfMaster)
    for i in ran:
        masterSocket.connect(masterIp + str(i))
    
    print("---------------------------------------------------------------")
    print("-- Client connected to all master processes successfully !!! --")
    print("---------------------------------------------------------------")

    for i in commands:
        # Getting path and command
        com = i.split()
        command=com[0]
        path=""
        for j in range(len(com)):
            if j==0:
                command=com[j]
            else:
                path+=com[j]
                if j != len(com)-1:
                    path+=" "

        if command=="upload" or command=="download":
            messege = None
            count=0
            while (messege == None and count < 25): # If all ports are busy send message 10 times
                masterSocket.send_pyobj([command,path])
                messege = masterSocket.recv_pyobj()
                time.sleep(0.2)
                count+=1
            if messege == None:     # If no reply received print timeout and continue
                print("Time out for request (",i,") !!!")
                continue
            datakeeperSocket.connect(str(messege[0])+str(messege[1]))
            if command=="upload":
                print ("Uploading "+ messege[2] + " to "+str(messege[0])+str(messege[1])+" ...")
                f= open(path,'rb')
                video=f.read()
                datakeeperSocket.send_pyobj([video,messege[2]])
                f.close()
                datakeeperSocket.recv()
                print("File uploaded successfully")
            else:
                print ("Downloading "+ messege[2] + " from "+str(messege[0])+str(messege[1])+" ...")
                datakeeperSocket.send_pyobj([path])
                video=datakeeperSocket.recv_pyobj()
                name=video[1].split("/")
                f=open(name[-1],'wb')
                f.write(video[0])
                f.close()
                # Sending success msg to master
                masterSocket.send_pyobj(["downloaded",str(messege[0]),str(messege[1])])
                masterSocket.recv_pyobj()
                print("Download completed successfully")
        else:
            print("Unknown command")
    datakeeperSocket.close()