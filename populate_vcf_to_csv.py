###############################################################################
Author = "Zhengxuan Wu"
Org = "Penn Medicine, University Of Pennsylvania"
Vrs = "1.0.0"
##################################DESCRIPTION##################################
'''This is VCF_DB/VEP_DB populator'''
'''Reference list'''
'''https://github.com/konradjk/loftee/blob/master/src/read_vep_vcf.py'''
#####################################LOG#######################################
Data_09_13_2016 = "Reading from vcf file, finding the header, parse the doc."
Data_09_16_2016 = "Adding the polyallelic feature into the parser"
####################################IMPORTS####################################
import gzip
import os
import sys
import csv
######################################MAIN#####################################
class csv_populator(object):

    def __init__(self, input_dir, input_filename, output_dir, output_filename):
        '''initialize function'''
        self.input_filename = input_filename
        self.input_dir = input_dir
        self.output_filename = output_filename
        self.output_dir = output_dir
        # data storage
        self.vep_field_names = None
        self.primary_field = ["CHROM", "POS", "REF", "ALT"]
        self.field = None
        # feature trigger
        self.polyallelic = "ON"
    
    def polyallelic_find(self, alt_list, ref_list):
        '''find polyallelic combinations, output as list of list[[A,B],[A,K]]'''
        final_combo_list = []
        for alt in alt_list:
            for ref in ref_list:
                final_combo_list.append([alt,ref])
        return final_combo_list
    
    def get_info_field(self):
        return self.vep_field_names
    
    def vcf_to_csv(self):
        '''main function to populate the csv file from vep data'''
        # file open for writing
        w = open(os.path.join(self.output_dir, self.output_filename),'w+')
        wr = csv.writer(w, dialect='excel')
        # creating headers for csv file
        csv_header = self.primary_field[:]
        csv_header = [element.lower() for element in csv_header]
        csv_header.append("annotations")
        wr.writerow(csv_header)
        # file open for reading
        f = open(os.path.join(self.input_dir, self.input_filename),'rb')
        for line in f:
            line = line.strip()
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
                print "VCF file does not have a VEP header line. Exiting."
                sys.exit(1)
            if self.field is None:
                print "VCF file does not have a header line for each colume. Exiting."
                sys.exit(1)
    
            # Reading the annotations after convert it to JSON
            sample = line.split('\t')
            info_field = sample[self.field["INFO"]]
            info_field = info_field.split("CSQ=")[-1]
            info_field_list = []
            for incident in info_field.split(","):
                info_field_list.append(dict(zip(self.vep_field_names, incident.split("|"))))

            # deal with polyallelic situation
            if len(sample[self.field["REF"]].split(",")) > 1 or len(sample[self.field["ALT"]].split(",")) > 1 and self.polyallelic == "ON":
                final_combo_list = self.polyallelic_find(sample[self.field["REF"]].split(","), sample[self.field["ALT"]].split(","))
                # creating multiple line here
                for possible_combo in final_combo_list:
                    sample[self.field["REF"]] = possible_combo[0];
                    sample[self.field["ALT"]] = possible_combo[1];
                    # Reading the primary key, form the final line output
                    data_line = []
                    for incident in self.primary_field:
                        data_line.append(sample[self.field[incident]])
                    data_line.append(info_field_list)
                    # Writing each line to csv file
                    wr.writerow(data_line)
            else:
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






