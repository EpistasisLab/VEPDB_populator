'''
CSV populator object to generate Cassandra-ready CSV from VEP VCF input.
'''

import gzip, os, sys, csv

class csv_populator(object):
    '''Class to generate populator object to convert VEP VCF input to Cassandra-ready CSV output.'''
    
    def __init__(self, input_filename, output_filename):
        '''Initialize function'''
        self.input_filename = input_filename
        self.output_filename = output_filename

        # data storage
        self.vep_field_names = None
        self.primary_field = ["CHROM", "POS", "REF", "ALT"]
        self.field = None
    
    def get_info_field(self):
        return self.vep_field_names
    
    def vcf_to_csv(self):
        '''Main method to populate the CSV file from a VEP VCF file'''
        # file open for writing
        w = open(self.output_filename, 'w')
        wr = csv.writer(w, dialect='excel')
        
        # creating headers for csv file
        csv_header = [x.lower() for x in self.primary_field[:]]
        csv_header.append("annotations")
        wr.writerow(csv_header)
                 
        # file open for reading
        f = open(self.input_filename, 'rb')
        for line in f:
            line = line.rstrip()
            # Reading header lines
            if line.startswith("#"):
                line = line.lstrip("#")
                # vep data
                if line.find("ID=CSQ") > -1:
                    self.vep_field_names = line.split('Format: ')[-1].strip('">').split('|')
                # field header
                elif line.startswith("CHROM"):
                    self.field = dict(zip(line.split(), range(len(line.split()))))
                continue

            # Error catching
            if self.field is None:
                print "VCF file does not have a VEP metadata line describing the CSQ annotations. Exiting."
                sys.exit(1)
            if self.field is None:
                print "VCF file does not have a header line describing each column. Exiting."
                sys.exit(1)
    
            # Reading the annotations after convert it to JSON
            sample = line.split('\t')
            info_field = sample[7]
            info_field = info_field.split("CSQ=")[-1]
            info_field_list = []
            for incident in info_field.split(","):
                info_field_list.append(dict(zip(self.vep_field_names, incident.split("|"))))

            # Reading the primary key, form the final line output
            data_line = []
            for incident in self.primary_field:
                data_line.append(sample[self.field[incident]])
            data_line.append(info_field_list)
            # Writing each line to csv file
            wr.writerow(data_line)

        # closing files
        w.close()
        f.close()






