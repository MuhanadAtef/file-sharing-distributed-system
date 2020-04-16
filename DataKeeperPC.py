import multiprocessing
#import Master
#import client
import DataKeeper


if __name__ == '__main__':
    
    ##############################################################################################################################################
    ################# PLEASE EITHER HARDCODE THE FOLLOWING IPS AND COMMENT THE FOLLOWING INPUT BLOCK OR INPUT THEM THROUGH CLI ###################
    ##############################################################################################################################################
    
    masterIP = "172.30.249.130"  # master ip
    datakeeperIP = "172.30.38.151"  # datakeeper ip
    
    ################################################################################
    ################################## INPUT BLOCK #################################
    ################################################################################
    
    print("/////////////////////////////////////////////////////////////////////")
    print("//////////////// Please enter the master server IP //////////////////")
    print("/////////////////////////////////////////////////////////////////////")
    masterIP = input()

    print("/////////////////////////////////////////////////////////////////////")
    print("////////////// Please enter the datakeeper server IP ////////////////")
    print("/////////////////////////////////////////////////////////////////////")
    datakeeperIP = input()
    
    ##############################################################################################################################################
    
    # number of processes  multi-process(MasterTracker)
    numberOfprocessesOfMaster = 5
    # number of processes  multi-process(data keeper)
    numberOfprocessesOfNodes = 3
    numberOfNodes = 1  # number of nodes of data keeper
    startingPortMasterClient = 7000  # first port between client/master
    startingPortDatakeeperClient = 8000  # first port between client/datakeeper
    
    replicatesCount = 3  # count of replicates
    processes = []

    for i in range(numberOfNodes):
        for k in range(numberOfprocessesOfNodes):
            t = multiprocessing.Process(target=DataKeeper.dataKeeper, args=(
                i, k, startingPortDatakeeperClient, numberOfprocessesOfMaster, masterIP, datakeeperIP))
            processes.append(t)

    for j in processes:
        j.start()

    for j in processes:
        j.join()
    print("Done!")
    while(True):
        []
