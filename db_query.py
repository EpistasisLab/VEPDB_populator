"""
Do a query on .vcf file against the database to get the .vep.vcf file

"""

import argparse, sys, os
from cassandra.cluster import Cluster
from multiprocessing import Pool
import time

keyspace_DB = 'vepdb_keyspace' # hard coded
table_DB = 'vepdb' # hard coded
contact_point_DB = ['127.0.0.1']

def by_line_query():
    raw_line = '7	84199033	rs549441037	A	G	.'

    cluster = Cluster(contact_points=contact_point_DB)
    db_session = cluster.connect()
    db_session.set_keyspace(keyspace_DB)

    line = raw_line.rstrip()
    annotation_list = line.split('\t')
    chrom = annotation_list[0]
    pos = annotation_list[1]  # omit the 3rd one
    ref = annotation_list[3]
    alt = annotation_list[4]

    print chrom, pos, ref, alt

    # select_statement = db_session.prepare("SELECT * FROM users WHERE id=?")
    #
    # statements_and_params = []
    # for user_id in user_ids:
    #     params = (user_id, )
    #     statements_and_params.append((select_statement, params))
    #
    # results = execute_concurrent(
    #     session, statements_and_params, raise_on_first_error=False)
    #
    # for (success, result) in results:
    #     if not success:
    #         handle_error(result)  # result will be an Exception
    #     else:
    #         process_user(result[0])  # result will be a list of rows

if __name__ == "__main__":
    by_line_query()