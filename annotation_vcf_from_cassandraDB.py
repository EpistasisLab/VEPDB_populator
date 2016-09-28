'''
    multi-processing data retrieving of VCF from cassandraDB
'''

# Some references:
# http://kmdouglass.github.io/posts/learning-pythons-multiprocessing-module.html
# http://stackoverflow.com/questions/20887555/dead-simple-example-of-using-multiprocessing-queue-pool-and-locking
# http://www.datastax.com/dev/blog/datastax-python-driver-multiprocessing-example-for-improved-bulk-data-throughput

'''Avoid using multi-threading, because in python it is serialized threading,
    pooled multi-processing will reduce the overhead, as well as maximize the
    parallel efficiency in the program'''


import gzip, os, sys, csv, re, multiprocessing
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

class annotator_vcf(object):
    '''annotator for plain vcf file, fetching data from cassandraDB'''

    def __init__(self, input_filename, output_filename, contact_point_DB, keyspace_DB, table_DB, concurrent_number):
        '''Initialize function'''
        self.input_filename = input_filename
        self.output_filename = output_filename
        self.contact_point_DB = contact_point_DB
        self.keyspace_DB = keyspace_DB
        self.table_DB = table_DB
        self.concurrent_number = concurrent_number

        # establish the connection to the database
        # entering the pre-defined keyspace
        cluster = Cluster(contact_points=self.contact_points)
        session = cluster.connect()
        session.set_keyspace(self.keyspace)
        
        # database variables
        self.primary_field = ["CHROM", "POS", "REF", "ALT"]
        self.primary_key = []
        self.fields = None
    
    def generate_primary_key(self);
        # file open for reading
        f = open(self.input_filename, 'rb')
        for line in f:
            line = line.rstrip()
            # Reading header lines
            if line.startswith("#"):
                line = line.lstrip("#")
                if line.startswith("CHROM"):
                    self.fields = dict(zip(line.split(), range(len(line.split()))))
                continue
            # Error catching
            if self.field is None:
                print "VCF file does not have a header line describing each column. Exiting."
                sys.exit(1)
                    
            # Reading lines and collect primary keys
            sample_primary_key = []
            sample = line.split('\t')
            for field in self.primary_field:
                sample_primary_key.append(sample[self.fields[field]])

            # Append to the primary key list
            self.primary_key.append(sample_primary_key)

    def run(self, sample):
        '''single process run function'''
        '''will return a single row in the whole table, iterative'''
        # isolation creation of the connection and executions
        profile_stmt = session.prepare("SELECT annotations From" + self.table_DB +\
                                       " Where chrom = ? AND pos = ? AND ref = ? \
                                       AND alt = ?")
        bound_stmt = profile_stmt.bind(sample)
        single_annotation_json = session.execute(bound_stmt)

        # TODO://
        
        # deserialize
        
        
        # combine and return
    
    
    def json_to_vep(annotation_json):
        '''deserialize the python json string format to a VEP format'''


    def multiprocessing_annotate(self):
        '''Main method to annotate the vcf file, and output a csv file'''
        # calling to generate the primary key
        self.generate_primary_key()
        
        # creating processing pools
        p = multiprocessing.Pool(self.concurrent_number)
        annotated_list = p.map(self.run(), self.primary_key)
        
        # file open for writing
        w = open(self.output_filename, 'w')
        wr = csv.writer(w, dialect='excel')
        
        # creating headers for csv file
        csv_header = [x.lower() for x in self.primary_field[:]]
        csv_header.append("annotations")
        wr.writerow(csv_header)
        
        # file open for reading
        for line in annotated_list:
            wr.writerow(line)


