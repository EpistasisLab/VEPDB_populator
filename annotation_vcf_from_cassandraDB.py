'''
    multi-processing data retrieving of VCF from cassandraDB
'''
# http://kmdouglass.github.io/posts/learning-pythons-multiprocessing-module.html


import gzip, os, sys, csv, re

class annotator_vcf(object):
    '''annotator for plain vcf file, fetching data from cassandraDB'''

    def __init__(self, input_filename, output_filename, contact_point_DB, keyspace_DB, table_DB):
        '''Initialize function'''
        self.input_filename = input_filename
        self.output_filename = output_filename
        self.contact_point_DB = contact_point_DB
        self.keyspace_DB = keyspace_DB
        self.table_DB = table_DB

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

    
    def annotate(self):
        '''Main method to annotate the vcf file, and output a csv file'''
        # file open for writing
        w = open(self.output_filename, 'w')
        wr = csv.writer(w, dialect='excel')
        
        # creating headers for csv file
        csv_header = [x.lower() for x in self.primary_field[:]]
        csv_header.append("annotations")
        wr.writerow(csv_header)
        
        # file open for reading
        f = open(self.input_filename, 'rb')


