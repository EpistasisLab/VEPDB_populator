###############################################################################
Author = "Zhengxuan Wu"
Org = "Penn Medicine, University Of Pennsylvania"
Vrs = "1.0.0"
##################################DESCRIPTION##################################
'''This is a CSV_to_DB populator'''
#####################################LOG#######################################
Data_09_16_2016 = "Connect to NoSQL DB, Copy Data"
Data_09_16_2016 = "line_type creation is changed"
####################################IMPORTS####################################
import gzip
import os
import sys
import csv
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
######################################MAIN#####################################
class db_populator(object):

    def __init__(self, keyspace, line_type, info_field, table_name, contact_points, file_name, file_path, test_flag):
        self.keyspace = keyspace
        self.line_type = line_type
        self.info_field = info_field
        self.table_name = table_name
        self.contact_points = contact_points
        self.file_name = file_name
        self.file_path = file_path
        self.test_flag = test_flag

    def csv_to_DB(self):
        print "Populating the database with copy command......"
        cluster = Cluster(contact_points=self.contact_points)
        session = cluster.connect()
        print "Connection to DB established......"

        # KEYSPACE config is different in testing local enviroment
        if self.test_flag:
            # create KEYSPACE:
            # https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html
            session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + self.keyspace +
                " WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east': 3}"
                #For multiple data centers, specify the replication factor of each one here.
                )
        else:
            # create KEYSPACE for testing, single local node
            session.execute(
            "CREATE KEYSPACE" + self.keyspace +
            "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"
            )
        session.set_keyspace(self.keyspace)
        print "KEYSPACE is created......"
        
        # Creating a new line type, based on info field from VEP data
        type_list = []
        final_string = ""
        for field in self.info_field:
            type_list.append(str(field) + " " + "text")
        final_string = " (" + ", ".join(type_list) + ")"
        
        session.execute(
                "CREATE TYPE IF NOT EXISTS " + self.line_type +
                final_string
                )

        # Compound key of (chromosome, position, ref, alt), the value is a list of the annotations.
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + self.table_name +
                "(chrom text, pos bigint, ref text, alt text, annotations list<frozen<" + self.line_type + ">>, " +
                "PRIMARY KEY  ((chrom, pos, ref, alt)))"
                #TODO: In the next version, we should use "PRIMARY KEY ((chrom), pos, ref, alt)"
                #so that we have both partition key and clustering keys inside primary key.
                #See https://jagadeeshs.wordpress.com/2015/07/15/cassandra/
                )

        # Copy csv file to DB
        print "COPY " + self.table_name + " (chrom, pos, ref, alt, annotations) FROM "\
            +  "'" +self.file_path + self.file_name + "'" + " WITH HEADER = ?"
        
        
        prepared_stmt = session.prepare("COPY " + self.table_name + " (chrom, pos, ref, alt, annotations) FROM "
                                            +  "'" +self.file_path + self.file_name + "'" + " WITH HEADER = ?"
                                            )
        bound_stmt = prepared_stmt.bind([TRUE])
        stmt = session.execute(bound_stmt)

        session.shutdown()