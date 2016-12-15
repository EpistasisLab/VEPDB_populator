import gzip
import re
import sys
from datetime import datetime
from threading import Thread
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

__author__ = 'Dichen Li MCIT, Yingjie Luan, Zhengxuan Wu, and Brian S. Cole PhD'

# This is the VEPDB populator. The schema of the DB is:
#   (chrom:text, pos:bigint, ref:text, alt:text, annotations: List<frozen<annotation>>)
#   where annotation is a user-defined data type in Cassandra with the VEP annotation fields.
#   (Allele: text, Consequence: text, IMPACT: text, SYMBOL: text, Gene: text, ...)
# A frozen list is like a tuple, it has fixed number of elements:
# https://docs.datastax.com/en/cql/3.3/cql/cql_reference/collection_type_r.html
#
# Currently this Python script only supports a single gzipped VEP VCF input file.
#
# Useful external documentation:
# https://cassandra.apache.org/doc/cql3/CQL-3.0.html
# https://github.com/datastax/spark-cassandra-connector/tree/master/doc

KEYSPACE = "vepdb_keyspace"
LINE_TYPE = "annotation"
TABLE = "vepdb"

file_name = sys.argv[1]
contact_points = sys.argv[2:]

print "Counting the number of lines in the file..."
lines = sum(1 for line in gzip.open(file_name, 'rb')) #84801901 from 1kGP, e.g.
print str(lines) + " lines"
threads = 4  # tested with different numbers, 4 is the best for the 9747 lines file
print "Populating the database with " + str(threads) + " threads"
cluster = Cluster(contact_points=contact_points)
session = cluster.connect()
print "Connection to DB established"

#Parse the consequence field names and to store them globally
def fieldname_generator():
    """
    :return: parsed field names (list)
    """
    raw_field_names = 'Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|REFSEQ_MATCH|GMAF|CLIN_SIG|SOMATIC|PHENO|GXA_EBV-transformed_lymphocyte|GXA_Experiment|GXA_adipose|GXA_adipose_tissue|GXA_adrenal|GXA_adrenal_gland|GXA_amygdala|GXA_animal_ovary|GXA_anterior_cingulate_cortex_(BA24)_of_brain|GXA_aorta|GXA_appendix|GXA_arm_muscle|GXA_artery|GXA_atrial_appendage_of_heart|GXA_bladder|GXA_bone_marrow|GXA_brain|GXA_breast|GXA_breast_(mammary_tissue)|GXA_bronchus|GXA_caudate_(basal_ganglia)_of_brain|GXA_caudate_nucleus|GXA_cerebellar_hemisphere_of_brain|GXA_cerebellum|GXA_cerebral_cortex|GXA_cerebral_meninges|GXA_cervix|GXA_cervix,_uterine|GXA_chronic_myelogenous_leukemia|GXA_colon|GXA_cord_blood|GXA_coronary_artery|GXA_cortex_of_kidney|GXA_diaphragm|GXA_diencephalon|GXA_duodenum|GXA_dura_mater|GXA_ectocervix|GXA_endometrium|GXA_epididymis|GXA_esophagus|GXA_esophagus_muscularis_mucosa|GXA_eye|GXA_fallopian_tube|GXA_frontal_cortex_(BA9)|GXA_frontal_lobe|GXA_gallbladder|GXA_gastroesophageal_junction|GXA_globus_pallidus|GXA_heart|GXA_heart_left_ventricle|GXA_heart_muscle|GXA_hippocampus|GXA_hypothalamus|GXA_kidney|GXA_large_intestine|GXA_lateral_ventricle|GXA_left_atrium|GXA_left_kidney|GXA_left_renal_cortex|GXA_left_renal_pelvis|GXA_left_ventricle|GXA_leg_muscle|GXA_leukocyte|GXA_liver|GXA_locus_coeruleus|GXA_lung|GXA_lymph_node|GXA_medulla_oblongata|GXA_middle_frontal_gyrus|GXA_middle_temporal_gyrus|GXA_minor_salivary_gland|GXA_mitral_valve|GXA_mucosa_of_esophagus|GXA_nasopharynx|GXA_nucleus_accumbens_(basal_ganglia)|GXA_occipital_cortex|GXA_occipital_lobe|GXA_olfactory_apparatus|GXA_oral_mucosa|GXA_ovary|GXA_pancreas|GXA_parathyroid_gland|GXA_parietal_lobe|GXA_parotid_gland|GXA_penis|GXA_pineal_gland|GXA_pituitary_gland|GXA_placenta|GXA_prefrontal_cortex|GXA_prostate|GXA_prostate_gland|GXA_pulmonary_valve|GXA_putamen|GXA_putamen_(basal_ganglia)|GXA_rectum|GXA_renal_cortex|GXA_renal_pelvis|GXA_right_renal_cortex|GXA_right_renal_pelvis|GXA_salivary_gland|GXA_seminal_vesicle|GXA_sigmoid_colon|GXA_skeletal_muscle|GXA_skin|GXA_skin_of_lower_leg|GXA_skin_of_suprapubic_region|GXA_small_intestine|GXA_smooth_muscle|GXA_soft_tissue|GXA_spinal_cord|GXA_spinal_cord_(cervical_c-1)|GXA_spleen|GXA_stomach|GXA_subcutaneous_adipose_tissue|GXA_submandibular_gland|GXA_substantia_nigra|GXA_temporal_lobe|GXA_terminal_ileum_of_small_intestine|GXA_testis|GXA_thalamus|GXA_throat|GXA_thymus|GXA_thyroid|GXA_thyroid_gland|GXA_tibial_artery|GXA_tibial_nerve|GXA_tongue|GXA_tonsil|GXA_trachea|GXA_transformed_fibroblast|GXA_transverse_colon|GXA_triscuspid_valve|GXA_trunk_muscle|GXA_umbilical_cord|GXA_urinary_bladder|GXA_uterus|GXA_vagina|GXA_vas_deferens|GXA_venous_blood|GXA_visceral_(omentum)_adipose_tissue|GXA_whole_blood|GO|CADD_PHRED|CADD_RAW|miRNA|ExAC_AF|ExAC_AF_AFR|ExAC_AF_AMR|ExAC_AF_Adj|ExAC_AF_CONSANGUINEOUS|ExAC_AF_EAS|ExAC_AF_FEMALE|ExAC_AF_FIN|ExAC_AF_MALE|ExAC_AF_NFE|ExAC_AF_OTH|ExAC_AF_POPMAX|ExAC_AF_SAS'
    raw_field_names = raw_field_names.split('|')
#   raw_field_names = [x.replace("(", "").replace(")", "").replace("-", "_").replace(',','') for x in raw_field_names] # remove parenthesis, replace -
    raw_field_names = ['"' + x + '"' for x in raw_field_names] #Wrap the column names in double quotes, which allows characters outside the range [_a-zA-Z]
    return raw_field_names

field_names = fieldname_generator()


def match_annotation(annotation):
    """Use regex to match a annotation line, and format a string ready to insert to Cassandra.
    Example input:
    CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212
    |retained_intron|||||||||||4136|1|HGNC|24023|1|2|3|4
    It has 25 fields, separated by '|'. In the future, there may be more plugins, in that case
    there will be more fields.
    For the example above, the method will return a string:
    "{vep: 'CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212
    |retained_intron|||||||||||4136|1|HGNC|24023|',
    lof: '1', lof_filter: '2', lof_flags: '3', lof_info: '4', others: 'None'}"
    """
    field_values = annotation.split('|')
    formatted_string = '{' #Start off string to return.
    if len(field_values) == len(field_names): #Good to go.
        for k, v in zip(field_names, field_values):
            v = re.sub('\'', '\'\'', v) #Escape embedded single quotes with another single quote: https://docs.datastax.com/en/cql/3.3/cql/cql_reference/escape_char_r.html
            formatted_string += "{}: '{}',".format(k, v)

        return formatted_string[:-1] + '}' #Close off string, removing final comma appended directly above.
    else:
        raise Exception("Failed to generate CQL from this string:\n{}".format(annotation))

def parse_line(raw_line):
    """Parse a line of raw VEP VCF data, convert to a tuple of 5 fields to present the 5 fields of cassandra DB.
    The first 4 fields are  chromosome, position, ref, alt. The last field (a string) represents
    the list of annotations compatible with CQL syntax.
    For example, an input:
    1	901994	G	A	CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|
    retained_intron|||||||||||4136|1|HGNC|24023||||,A|upstream_gene_variant|MODIFIER|PLEKHN1|ENSG00000187583|
    Transcript|ENST00000480267|retained_intron|||||||||||4261|1|HGNC|25284||||
    Will be converted to (without line break or indent):
    (1, 901994, 'G', 'A',
        "[{
            Allele: 'A',
            Consequence: 'downstream_gene_variant',
            IMPACT: 'MODIFIER',
            Symbol: 'KLHL17',
            Gene: 'ENSG00000187961',
            ...
          },
          {
            Allele: 'A',
            Consequence: 'upstream_gene_variant',
            IMPACT: 'MODIFIER',
            Symbol: 'PLEKHN1',
            Gene: 'ENSG00000187583',
            ...
          }]"
    )
    """
    fields = raw_line.split("\t")
    chrom, pos, ref, alt = fields[0], fields[1], fields[3], fields[4]
    annotations_str_list = fields[7].lstrip("CSQ=").split(',') #Remove start of string, preserving only comma-delimited annotations.
    annotations = map(match_annotation, annotations_str_list)
    if annotations.__contains__(None):
        return None  # wrong formatted line
        #Raise an exception here?
    #Return a tuple of the 5 values: 4 are primary key, last is the CQL-formatted annotation string:
    return chrom, long(pos), ref, alt, "[" + ", ".join(annotations) + "]"


def insert(raw_line, db_session):
    """Parse a raw line to compose CQL query and execute it to insert the line"""
    if raw_line.startswith('#'):
        return True #Skip header/metadata lines.

    parsed = parse_line(raw_line)
    if parsed is None:
        return False
    insert_statement = db_session.prepare(
        "INSERT INTO " + KEYSPACE + '.' + TABLE +
        " (chrom, pos, ref, alt, annotations) VALUES" +
        " (?, ?, ?, ?, " + parsed[4] + ")"
    )
    query = insert_statement.bind(parsed[:4])
    query.consistency_level = ConsistencyLevel.ALL #Require ALL for consistency level.

    # example query:
    # INSERT INTO vep_db (chrom, pos, ref, alt, annotations) VALUES
    # ('1', 901994, 'G', 'A', [{vep: 'foo', lof:'', lof_filter:'', lof_flags: '', lof_info: '', other_plugins: ''}])
    db_session.execute(query)
    return True


def populate_db(t_idx):
    """
    Run by one thread to populate the database.
    :param t_idx: the thread index, in range(0, threads)
    :return: None
    """
    f = gzip.open(file_name, 'rb')  # read only access, so thread safe
    count = 0  # line number in the file
    inserted = 0  # number of lines inserted by this thread
    bad_count = 0  # number of lines not inserted because of bad format
    bad_lines = []

    start_time = datetime.now()
    for line in f:
        count += 1
        if count % threads != t_idx:
            continue
        # Multiple threads are sharing the same session.
        # using more than one session for one key space is not good: http://goo.gl/lkH0rR
        # also session object is thread safe: http://goo.gl/rb9QTp
        if not insert(line, session):
            bad_count += 1
            bad_lines.append(line)
        else:
            inserted += 1
        if inserted % 1000 == 0 and inserted > 0: # prints after each 1000 lines of inserts
            percent = float(count) * 100 / lines
            time_left = (datetime.now() - start_time) * (lines - count) / count
            print "Thread #" + str(t_idx) + ": " + str(percent) + "% done, est. time left: " + str(time_left)
    f.close()
    print "Thread #" + str(t_idx) + ": " + str(inserted) + \
          " rows inserted, time spent: " + str(datetime.now() - start_time)
    print "Thread #" + str(t_idx) + ": " + "Bad lines: " + str(bad_count)
    for line in bad_lines:
        print line


pool = []
for i in range(0, threads):
    t = Thread(target=populate_db, args=(i,))
    t.start()
    pool.append(t)

for t in pool:
    t.join()

session.shutdown()
