"""
Do a query on .vcf file against the database to get the .vep.vcf file

"""

import argparse, sys, os
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
import multiprocessing

input_filename = './t/test.vcf'
target_filename = 'results.txt'
keyspace_DB = 'vepdb_keyspace' # hard coded
table_DB = 'vepdb' # hard coded
contact_point_DB = ['127.0.0.1']

def file_reader(file_name):
    """
    Read in input file and prepare the CQL statement
    :param file_name:
    :return: return a list of statements
    """
    statements = []
    with open(file_name) as f:
        for line in f:
            if not line.startswith("#"):
                line = line.rstrip()
                annotation_list = line.split('\t')
                chrom = annotation_list[0]
                pos = annotation_list[1]  # omit the 3rd one
                ref = annotation_list[3]
                alt = annotation_list[4]
                query_str = (
                "SELECT * FROM vepdb_keyspace.vepdb WHERE chrom = '{0}' AND pos = {1} AND ref = '{2}' AND alt = '{3}'".format(
                    chrom, pos, ref, alt), ())
                statements.append(query_str)
    return statements


def do_query(statements):
    """
    Do a multiprocessing query using native API
    :param statements:
    :return: return CQL a list of Resultset object
    """
    cluster = Cluster(contact_points=contact_point_DB)
    db_session = cluster.connect()
    db_session.set_keyspace(keyspace_DB)
    results = execute_concurrent(db_session, statements, raise_on_first_error=False)
    return results


def result_vep(result):
    """
    Render each result
    :param result:
    :return: change result object into vep format string
    """
    (success, result) = result
    if not success:
        print "Query Failed"
    else:
        result = result[0]
        key = str(result[0]) + '\t' + str(result[1]) + '\t' + str(result[2]) + '\t' + str(result[3])
        container = []
        for item in result[4]:
            container.append('|'.join(item))
        annotation = ','.join(container)
        return key + annotation + '\n'


def mp_handler(result_set):
    """
    Handle multiprocessing writing to target file
    :param result_set:
    :return:
    """
    p = multiprocessing.Pool(4)
    with open(target_filename, 'w') as f:
        for re in p.imap(result_vep, result_set):
            f.write(re)

if __name__ == "__main__":
    db_statements = file_reader(input_filename)
    db_results = do_query(db_statements)

    # Single Thread version:
    with open(target_filename, 'w') as f:
        for item in db_results:
            f.write(result_vep(item))

    # Multithread version: not working
    # mp_handler(results)


