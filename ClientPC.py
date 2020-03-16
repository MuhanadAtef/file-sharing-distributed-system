import multiprocessing 
import Client
    

if __name__ == '__main__':
    numberOfprocessesOfMaster=5 #number of processes  multi-process(MasterTracker)
    startingPortMasterClient=7000 #first port between client/master
    startingPortDatakeeperClient=8000 #first port between client/datakeeper
    masterIp="tcp://172.30.249.130:" #master ip
    processes=[]
    
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
        t=multiprocessing.Process(target=Client.client,args=(masterIp,startingPortMasterClient,numberOfprocessesOfMaster,commands))
        processes.append(t)

    for j in processes:
        j.start()
        
    for j in processes:
        j.join()