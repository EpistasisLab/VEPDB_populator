'''
    multi-processing data retrieving of VCF from cassandraDB
    Version 2
'''

import gzip, os, sys, csv, re, multiprocessing
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

class populate_vcf(object):
    '''annotator for plain vcf file, fetching data from cassandraDB'''

    def __init__(self, input_filename=None, contact_point_DB=None, keyspace_DB=None):
        '''Initialize function'''
        self.input_filename = input_filename
        # self.output_filename = output_filename
        # self.contact_point_DB = contact_point_DB
        # self.keyspace_DB = keyspace_DB
        # self.table_DB = table_DB
        # self.concurrent_number = concurrent_number

        #establish the connection to the database
        #entering the pre-defined keyspace
        cluster = Cluster(contact_points=contact_point_DB)
        self.session = cluster.connect()
        print "Connection to DB established"
        self.session.set_keyspace(keyspace_DB)


    def vcf_parser(self):
        '''Main method to populate the CSV file from a VEP VCF file'''

        f = open(self.input_filename, 'rb')

        for line in f:
            line = line.rstrip()
            print line
            # Reading header lines
            # if line.startswith("#"):
            #     line = line.lstrip("#")
            #     # vep data
            #     if line.find("ID=CSQ") > -1:
            #         raw_field_names = line.split('Format: ')[-1].strip('">').split('|')
            #         self.vep_field_names = [self._remove_nonalpha(x) for x in
            #                                 raw_field_names]  # Cassandra can't support nonalphanumeric chars.
            #     # field header
            #     elif line.startswith("CHROM"):
            #         self.field = dict(zip(line.split(), range(len(line.split()))))
            #     continue
            #
            # # Error catching
            # if self.field is None:
            #     print "VCF file does not have a VEP metadata line describing the CSQ annotations. Exiting."
            #     sys.exit(1)
            # if self.field is None:
            #     print "VCF file does not have a header line describing each column. Exiting."
            #     sys.exit(1)
            #
            # # Reading the annotations after convert it to JSON
            # sample = line.split('\t')
            # info_field = sample[7]
            # info_field = info_field.split("CSQ=")[-1]
            # info_field_list = []
            # for incident in info_field.split(","):
            #     info_field_list.append(dict(zip(self.vep_field_names, incident.split("|"))))
            #
            # # Reading the primary key, form the final line output
            # data_line = []
            # for incident in self.primary_field:
            #     data_line.append(sample[self.field[incident]])
            # data_line.append(info_field_list)

if __name__ == "__main__":
    a = populate_vcf("./t/test.vep.vcf", contact_point_DB = ['127.0.0.1'], keyspace_DB = 'vepdb_keyspace')
    a.vcf_parser()