# -*- coding: utf-8 -*-

# Functions and Script to extract data

import blocksci
import pandas as pd
import numpy as np
import networkx as nx
import multiprocessing as mp
import itertools
import random
import time
import string
import pickle
import csv
import gc
import os, sys
from functools import partial



#***********CLASSES AND FUNTIONS***********

# Class that creates a blockchain a blockchain partition (dictionary) given data range and partition type (blocks,days,weeks)

class BchainPartition():

    def __init__(self,chain,start_timestamp,end_timestamp,ptype='blocks',sample_size=10):
        blocks=chain.range(start=start_timestamp,end=end_timestamp)


        self.block_h=blocks.height
        print('Start_block: {}'.format(self.block_h[0]))
        print('End_block: {}'.format(self.block_h[-1]))

        if sample_size>0: #Samples blocks from the
            sample_list=list(np.random.choice(self.block_h,sample_size))
            sample_blocks=[chain[ix_b] for ix_b in sample_list]
            txs=[b.txes for b in sample_blocks]
            self.partition={h:[t for t in t_l] for h,t_l in zip(sample_list,txs)}
            self.no_parts=len(sample_blocks)

        else:
            if ptype=='blocks':
                self.partition={b.height:[tx for tx in b.txes] for b in blocks}
                self.no_parts=np.int32(len(blocks))


        print('Number of Blocks: {} '.format(len(blocks)))
        print('Highest block height: {}'.format(blocks[-1].height))
        print('Number of Transactions: {} '.format(len(txs)))

        # ***TODO: Create partition for other types of partitions (use tx.block_time)

# Function that takes blockchain partition and outputs pandas data frame with features
# for the graph defined by each split in the partition

def partition_data(chainpartiton,directory,filename):

    # Dictionary with partition
    partition=chainpartiton.partition
    partindex=partition.keys()
    parts=partition.values()
    data_tuples=[]
    graphs=[]

    print('Number of parts: {}'.format(len(partindex)))

    tuples=[(index,part) for index,part in zip(partindex,parts)]
    no_parts=len(tuples)
    processed=0

    for t in tuples:

        data_i,columns_i,graph_i=graph_features(t,slice_type='blocks')

        with open(filename,'a') as f:
            writer = csv.writer(f, delimiter=',')
            if len(data_tuples)==0: # Write column names on first pass
                writer.writerow(columns_i)
            writer.writerow(data_i)
        # Save graph
        nx.write_gpickle(graph_i,directory+str(graph_i.graph['graph_id'])+'.gpickle')

        data_tuples.append((data_i,columns_i))
        graphs.append(graph_i)
        processed+=1
        progress=(processed/no_parts)*100
        #sys.stdout.write("Download progress: %d%%   \r" % (progress) )
        sys.stdout.write("Download progress: {:07.4f}   \r".format(progress) )
        sys.stdout.flush()

    '''
    chunksize=len(tuples)%ncpu
    with mp.Pool(processes=ncpu) as pool:
        data_tuples=pool.map(graph_features,tuples,chunksize)

    '''

    columns=data_tuples[0][1] #This value is being re-written. This design choice is to mantain consistency with columns.
    data=[i for i,j in data_tuples]
    data=np.array(data)
    df=pd.DataFrame(data=data[:,:],columns=columns)


    return (df,graphs)

# Function that receives a chain part (list of transactions), generates transaction graph and calculates statistics

def graph_features(chain_part_tuple,slice_type='blocks'):

    index=chain_part_tuple[0]
    chain_part=chain_part_tuple[1]
    block_height=chain_part[-1].block_height
    graph=block_graph(chain_part,index,slice_type)
    nx.info(graph)
    nodes=graph.nodes(data=True)
    edges=graph.edges(data=True)
    data=[index]
    columns=['block_height']

    # Number of Nodes
    no_nodes=nx.number_of_nodes(graph)
    data.append(no_nodes)
    columns.append('no_nodes')

    # Number of Edges (address to address transactions)
    no_edges=nx.number_of_edges(graph)
    data.append(no_edges)
    columns.append('no_edges')

    # Total value transacted
    total_value=np.sum(np.array([a['value'] for n1,n2,a in edges]))
    data.append(total_value)
    columns.append('value_transacted')

    # Total Density
    density=nx.density(graph)
    data.append(density)
    columns.append('total_density')

    # Nodes with self loops nx.loops nodes_with_selfloops(G) nodes_with_selfloops(G)
    nodes_self=nx.number_of_selfloops(graph)
    data.append(nodes_self)
    columns.append('nodes_self')

    # Value of self loops nodes_with_selfloops(G)
    values=np.array([a['value'] for n1,n2,a in nx.selfloop_edges(graph,data=True)])
    selfloop_value=np.sum(values)
    data.append(selfloop_value)
    columns.append('selfloop_value')

    # Number of transactions to old addresses

    old_nodes=[n for n,a in nodes if a['block_created']<block_height]
    edges_to_old=graph.in_edges(old_nodes,data=True)
    data.append(len(edges_to_old))
    columns.append('old_nodes_in')

    # Ratio of transactions to old addresses to total transactions

    ratio_oldin_totalin=len(edges_to_old)/(no_edges+1)
    data.append(ratio_oldin_totalin)
    columns.append('ratio_oldin_totalin')

    # Value of transactions to old addresses

    value_to_old=[a['value'] for n1,n2,a in edges_to_old]
    data.append(np.sum(np.array(value_to_old)))
    columns.append('value_to_old')

    # Old address density

    old_graph=nx.induced_subgraph(graph,old_nodes)
    old_density=nx.density(old_graph)
    data.append(old_density)
    columns.append('old_density')


    # ***TODO*** (Aggregated graph analysis)

    # Accumulated reuse
    # Dominance (Agg graph or new vs. old dominance)
    #https://networkx.github.io/documentation/stable/reference/algorithms/dominance.html
    # Common ancenstors (as with dominance the address ancestor path should be proportional
    #to the blockchain lenght if address reuse is minimal)
    #***********

    #print('{} Processed'.format(index))
    return (data,columns,graph)

# Function that creates transaction graph for a given number transactions


def block_graph(txs,index,slice_type):


    # Create graph and process
    graph = nx.MultiDiGraph(graph_id=index,slice_type=slice_type)
    nodes=[]
    edges=[]


    # Extract transactions information

    init_block=txs[0].block.height
    txs_dic={tx.index:tx for tx in txs}
    txs_ix=list(txs_dic.keys())
    txs_ix.sort()


    start_ix=txs_ix[0]
    end_ix=txs_ix[-1]


    # Generate edges to input to graph

    # TODO:Re-write for pre-process: See last answ with qeues https://stackoverflow.com/questions/33107019/multiple-threads-writing-to-the-same-csv-in-python
    '''
    with mp.Pool(processes=ncpu) as pool:
        edges=pool.map(extract_nodes_edges,txs,chunksize)


    '''
    for tx in txs:
        edges_i,nodes_i=extract_nodes_edges(tx)
        nodes.append(nodes_i)
        edges.append(edges_i)


    nodes=list(itertools.chain.from_iterable(nodes))
    edges=list(itertools.chain.from_iterable(edges))

    # Input to graph

    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)


    #print('Generated Graph for Block starting at:{}'.format(init_block))
    return graph

# Function that receives a transaction and generates nodes and edges from addresses in transaction

def extract_nodes_edges(transaction):

    # Initialize values and get info from transaction
    edges=[]
    output_value=transaction.output_value
    block_height=transaction.block_height
    tx_id=transaction.index

    # Get inputs, types and values
    inputs=transaction.inputs.address
    input_val=transaction.inputs.value
    input_nodes=[(inp.address_num,{'raw_type':inp.raw_type,'block_created':inp.first_tx.block.height})for inp in inputs]

    # Get outputs and types
    outputs=transaction.outputs.address
    output_nodes=[(out.address_num,{'raw_type':out.raw_type,'block_created':out.first_tx.block.height})for out in outputs]

    # ****TODO: Add address balance as attribute to node****

    # Create nodes

    nodes=input_nodes+output_nodes

    # Create edges (NetworkX will automatically create nodes when given edges)

    for i in range(len(inputs)):
        value=input_val[i]
        prop_value=value/len(outputs)

        for o in range(len(outputs)):
            edge=(inputs[i].address_num,outputs[o].address_num,{'value':prop_value,'tx_id':block_height})
            edges.append(edge)

    return edges,nodes


#***********SCRIPT***********

# Point to parsed blockchain data
ncpu=mp.cpu_count()
chain = blocksci.Blockchain("/home/ubuntu/bitcoin")
types=blocksci.address_type.types
total_blocks=chain.blocks
print('Total Blocks up to {}:  {} '.format(total_blocks[-1].time,len(total_blocks)))

#---SCRIPT: generates data for graphs in each part of the partition

# Create directories and files to store graphs and dataframe

# Generate an extraction ID (Each id has random id)
extraction_id = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(6)])
print('Extraction id: {}'.format(extraction_id))

#---Save Dataframes

# Create directory and save

start='2010-02-01 00:00:00'
end='2018-02-01 11:59:59'
blocks=chain.range(start=start,end=end)
sample_size=35000

start_c=start
start_c=start_c.replace('-','_').replace(' ','_').replace(':','_')
end_c=end
end_c=end_c.replace('-','_').replace(' ','_').replace(':','_')

directory='extractions/'+extraction_id+'-'+str(sample_size)+'-blocks-'+start_c+'-'+end_c+'/graphs'+'/'


if not os.path.exists(directory):
    os.makedirs(directory)

# Create Filename and save

filename='extractions/'+extraction_id+'-'+str(sample_size)+'-blocks-'+start_c+'-'+end_c+'/'+extraction_id+'-'+str(sample_size)+'-blocks-'+start_c+'-'+end_c+'.csv'


start_time=time.time()

partition=BchainPartition(chain,start,end,sample_size=sample_size)
df,graphs=partition_data(partition,directory,filename)
df.head()
end_time=time.time()
print('Time taken={}'.format(end_time-start_time))
print('\n***EXTRACTION COMPLETED SUCCESSFULLY***')
