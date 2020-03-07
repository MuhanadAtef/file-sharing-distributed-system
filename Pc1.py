import multiprocessing 
import Master    
import client
import DataKeeper
    

if __name__ == '__main__':
    numberOfprocessesOfMaster=5 #number of processes  multi-process(MasterTracker)
    numberOfprocessesOfNodes=2 #number of processes  multi-process(data keepe)
    numberOfNodes=2 #number of nodes of data keeper
    startingPortMasterClient=7000 #first port between client/master
    masterIp="tcp://localhost:" #master ip
    processes=[]
    
    for k in range(numberOfprocessesOfMaster):
        t= multiprocessing.Process(target=Master.masterTracker,args=(k,numberOfNodes)) 
        processes.append(t)
    for i in range(numberOfNodes):
        for k in range(numberOfprocessesOfNodes):
            t= multiprocessing.Process(target=DataKeeper.dataKeeper,args=(i,k,numberOfprocessesOfNodes)) 
            processes.append(t)

    print("Enter # of client processes:")
    numberOfClients=input()
    for i in range(int(numberOfClients)):
        commands=[]
        print("Enter # of commands for process #%d"%i)
        n=input()
        for j in range(int(n)):
            print("command #%d:"%j)
            x=input()
            commands.append(x)
        print(commands)
        t=multiprocessing.Process(target=client.client,args=(masterIp,startingPortMasterClient,numberOfprocessesOfMaster,commands))
        processes.append(t)

    for j in processes:
        j.start()
        
    for j in processes:
        j.join()
    print("Done!")
    while(True):
        []