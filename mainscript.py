###############################################################################
Author = "Zhengxuan Wu"
Org = "Penn Medicine, University Of Pennsylvania"
Vrs = "1.0.0"
##################################DESCRIPTION##################################
'''This is the main script that run populator'''
#####################################LOG#######################################
Data_09_16_2016 = "First write up the main script to run the populator"
####################################IMPORTS####################################
import sys
from populate_vcf_to_csv import csv_populator
from populate_csv_to_DB import db_populator
#####################################MODES#####################################
test = True
hasCSV = True
######################################MAIN#####################################

'''Specify the dir'''

# getting the input and output directory from the command line

input_filename = sys.argv[1]
output_filename = sys.argv[2]

input_dir = ''
output_dir = ''

populator = csv_populator(input_dir, input_filename, output_dir, output_filename)
populator.vcf_to_csv()

#'''copy to DB'''
#KEYSPACE = "dage"
#LINE_TYPE = "vep_annotation"
#INFO_FIELD = populator.get_info_field()
#TABLE = "vep_db"
#if test == True:
#    CONTACT_POINT = ['127.0.0.1']
#else:
#    CONTACT_POINT = ""
#db_populator_instance = db_populator(KEYSPACE, LINE_TYPE, INFO_FIELD, TABLE, \
#                                     CONTACT_POINT, output_filename, \
#                                     output_dir, test)
#db_populator_instance.csv_to_DB()