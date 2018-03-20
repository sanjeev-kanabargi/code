#!/usr/bin/env python

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from time import strftime
import time

try:
    import configparser
except:
    from six.moves import configparser

import argparse

# Note: You MUST at least enter valid AWS API Keys in this file:


# Usage/Setup:
# 1. Put your AWS creds in the CONFIG_FILE file located in the same subdirectory as this file
# (This step is MANDATORY)
#
# 2. Review the other config params in the CONFIG_FILE.
# The default params should be okay in most cases, but take a look to be sure.


def snapshot_indices_from_src_to_s3(args):
    """
    Take a snapshot of all the indices specified in the config file.

    The specified indices are backed up from the ElasticSearch Node on which backup is initiated
    and are stored at the S3 location specified in the config file.

    Parameters:
        config: dictionary storing the configuration details

    """
    
    src_hosts = args.hosts.split(',')
    es_s3_repo = args.repository

    try:
		src_es = Elasticsearch(src_hosts, sniff_on_start=True,
            sniff_on_connection_fail=True, sniffer_timeout=60
            ,timeout=30, max_retries=3, retry_on_timeout=True)
		print ("\n[INFO] Connected to src ES cluster: %s" %(src_es.info()))
		epocTime = str(int(time.time()))
		indices = args.indices
		

		finalIndices = []
		if indices == 'all' :
			#Fetch all indices from elasticsearch.
			for index in src_es.indices.get('*'):
				if(index.startswith('.') != True):
					finalIndices.append(index.encode('ascii', 'ignore'))
			indices = str(finalIndices).strip('[]').replace('\'',"").replace(" ","")
		print ("\n[INFO] Snapshotting ES indices: '%s' to S3...\n" %indices)
		snapShotName = args.snapshot
		print 'Snapshot name : ',snapShotName+epocTime
		response = src_es.snapshot.create(repository=es_s3_repo,
            	snapshot=snapShotName+epocTime,
            	body={"indices": indices},
            	wait_for_completion=True)
		
		print 'response : ',response

    except Exception as e:
        print ("\n\n[ERROR] Unexpected error: %s" %(str(e)))
	raise Exception(str(e))



def restore_indices_from_s3_to_dest(args):
    
    """
    	Restore the specified indices from the snapshot specified in the config file.

	    The indices are restored at the specified 'dest' ElasticSearch Node.
    	ElasticSearch automatically replicates the indices across the ES cluster after the restore.
	
    """
    


    dest_seed1 = args.seed1
    es_s3_repo = args.repository
    index_list = args.indices
    snapShot = args.snapshot
    
    

    if args.seed2 == None :
        dest_seed2 = dest_seed1
    
    if args.seed3 == None :
        dest_seed3 = dest_seed1
   
    
    try:
        dest_es = Elasticsearch([dest_seed1, dest_seed2, dest_seed3], sniff_on_start=True,sniff_on_connection_fail=True, sniffer_timeout=60)
        
        

        if index_list == None :
            index_list = dest_es.snapshot.get(es_s3_repo,snapShot)['snapshots'][0]['indices']
        
        print ("\n[INFO] Snapshotting ES indices: '%s' to S3...\n" %index_list)
        resp = dest_es.cluster.health()

        print ("\n[INFO] Connected to dest ES cluster: %s" %(dest_es.info()))

        # must close indices before restoring:
        for index in index_list:
            try:
                print ("[INFO] Closing index: '%s'" %(index))
                dest_es.indices.close(index=index, ignore_unavailable=True)
            except NotFoundError:
                print ("\n\n[WARN] Index '%s' not present on Target ES cluster - could not close it." %(index))
            except Exception as e:
                print ("\n\n[ERROR] Unexpected error '%s' while trying to close index: '%s'" %(str(e)))
                #reopen_indices(dest_es, index_list)

        print ("\n[INFO] Restoring ES indices: '%s' from S3 snapshot...\n" %index_list)

        dest_es.snapshot.restore(repository=es_s3_repo,
            snapshot=snapShot,
            body={"indices": index_list},
            wait_for_completion=False)

    except Exception as e:
        print ("\n\n[ERROR] Unexpected error: %s" %(str(e)))
        print e
    finally:
        print ("\n[INFO] (finally) Re-opening indices: '%s'" %(str(index_list)))
        reopen_indices(dest_es, index_list)



def reopen_indices(es, index_list):
    """
    Re-open indices
    (used to ensure indices are re-opened after any restore operation)

    Parameters:
        es         : ElasticSearch connection object
        index_list : List of ElasticSearch indices that needs to be open
    """

    try:
        for index in index_list:
            print ("[INFO] reopen_indices(): Opening index: '%s'" %(index))
            es.indices.open(index=index, ignore_unavailable=True)
    except NotFoundError:
                print ("\n\n[WARN] Could not reopen missing index on Target ES cluster: '%s'" %(index))
    except Exception as e:
        print ("\n\n[ERROR] Unexpected error in reopen_indices(): %s" %(str(e)))


def main():

    parser = argparse.ArgumentParser(description='Push specified Elasticsearch indices from SOURCE to DESTINATION as per config in the `es-s3-snapshot.conf` file.')
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-m', '--mode',help="Mode of operation. Choose 'backup' on your SOURCE cluster. \Choose 'restore' on your DESTINATION cluster",choices=['backup','restore'], required=True)
    requiredNamed.add_argument('-r', '--repository',help="Repository name, must be same as registred",required=True)
    requiredNamed.add_argument('-ho', '--hosts',help="Elasticsearch master hosts, comma seperated values",required=True)
    requiredNamed.add_argument('-i', '--indices',help="indices, seperated by comma, leave blank to add all indices",required=False)
    requiredNamed.add_argument('-s', '--snapshot',help="Value to add prefix to snapshot name ",required=True)
    args = parser.parse_args()

    

    if args.mode == 'backup':
        snapshot_indices_from_src_to_s3(args)
    


    if args.mode == 'restore':
        restore_indices_from_s3_to_dest(args)

    print "Finished!!"


if __name__ == "__main__":
    	main()

