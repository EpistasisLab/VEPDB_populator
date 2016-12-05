'''
    Batch populating cassandraDB using multiprocessing
    example of usage:

    python db_populator.py -i "./t/test.vep.vcf" -p 4 -c "127.0.0.1"

    Time Used:  10.5361790657
'''

import argparse, sys, os
from cassandra.cluster import Cluster
from multiprocessing import Pool
import time

# input_filename = './t/test.vep.vcf'
# contact_point_DB = []
keyspace_DB = 'vepdb_keyspace' # hard coded
table_DB = 'vepdb' # hard coded

def getargs():
    """

    Collect input and output files.
    """
    parser = argparse.ArgumentParser(description=" Batch populating cassandraDB using multiprocessing.")
    parser.add_argument('-i', '--input_file', required=True, help="Input VEP VCF file to convert to CSV.")
    parser.add_argument('-c', '--contact_point', required=True, help="Cassandra contact point")
    parser.add_argument('-p', '--processing_number', required=True, help="Number of workers")
    args = parser.parse_args()
    return args

# parse the field name and to store it globally
def fieldname_generator():
    """

    :return: parsed field name
    """
    raw_field_names = 'Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|REFSEQ_MATCH|GMAF|CLIN_SIG|SOMATIC|PHENO|GXA_EBV-transformed_lymphocyte|GXA_Experiment|GXA_adipose|GXA_adipose_tissue|GXA_adrenal|GXA_adrenal_gland|GXA_amygdala|GXA_animal_ovary|GXA_anterior_cingulate_cortex_(BA24)_of_brain|GXA_aorta|GXA_appendix|GXA_arm_muscle|GXA_artery|GXA_atrial_appendage_of_heart|GXA_bladder|GXA_bone_marrow|GXA_brain|GXA_breast|GXA_breast_(mammary_tissue)|GXA_bronchus|GXA_caudate_(basal_ganglia)_of_brain|GXA_caudate_nucleus|GXA_cerebellar_hemisphere_of_brain|GXA_cerebellum|GXA_cerebral_cortex|GXA_cerebral_meninges|GXA_cervix|GXA_cervix,_uterine|GXA_chronic_myelogenous_leukemia|GXA_colon|GXA_cord_blood|GXA_coronary_artery|GXA_cortex_of_kidney|GXA_diaphragm|GXA_diencephalon|GXA_duodenum|GXA_dura_mater|GXA_ectocervix|GXA_endometrium|GXA_epididymis|GXA_esophagus|GXA_esophagus_muscularis_mucosa|GXA_eye|GXA_fallopian_tube|GXA_frontal_cortex_(BA9)|GXA_frontal_lobe|GXA_gallbladder|GXA_gastroesophageal_junction|GXA_globus_pallidus|GXA_heart|GXA_heart_left_ventricle|GXA_heart_muscle|GXA_hippocampus|GXA_hypothalamus|GXA_kidney|GXA_large_intestine|GXA_lateral_ventricle|GXA_left_atrium|GXA_left_kidney|GXA_left_renal_cortex|GXA_left_renal_pelvis|GXA_left_ventricle|GXA_leg_muscle|GXA_leukocyte|GXA_liver|GXA_locus_coeruleus|GXA_lung|GXA_lymph_node|GXA_medulla_oblongata|GXA_middle_frontal_gyrus|GXA_middle_temporal_gyrus|GXA_minor_salivary_gland|GXA_mitral_valve|GXA_mucosa_of_esophagus|GXA_nasopharynx|GXA_nucleus_accumbens_(basal_ganglia)|GXA_occipital_cortex|GXA_occipital_lobe|GXA_olfactory_apparatus|GXA_oral_mucosa|GXA_ovary|GXA_pancreas|GXA_parathyroid_gland|GXA_parietal_lobe|GXA_parotid_gland|GXA_penis|GXA_pineal_gland|GXA_pituitary_gland|GXA_placenta|GXA_prefrontal_cortex|GXA_prostate|GXA_prostate_gland|GXA_pulmonary_valve|GXA_putamen|GXA_putamen_(basal_ganglia)|GXA_rectum|GXA_renal_cortex|GXA_renal_pelvis|GXA_right_renal_cortex|GXA_right_renal_pelvis|GXA_salivary_gland|GXA_seminal_vesicle|GXA_sigmoid_colon|GXA_skeletal_muscle|GXA_skin|GXA_skin_of_lower_leg|GXA_skin_of_suprapubic_region|GXA_small_intestine|GXA_smooth_muscle|GXA_soft_tissue|GXA_spinal_cord|GXA_spinal_cord_(cervical_c-1)|GXA_spleen|GXA_stomach|GXA_subcutaneous_adipose_tissue|GXA_submandibular_gland|GXA_substantia_nigra|GXA_temporal_lobe|GXA_terminal_ileum_of_small_intestine|GXA_testis|GXA_thalamus|GXA_throat|GXA_thymus|GXA_thyroid|GXA_thyroid_gland|GXA_tibial_artery|GXA_tibial_nerve|GXA_tongue|GXA_tonsil|GXA_trachea|GXA_transformed_fibroblast|GXA_transverse_colon|GXA_triscuspid_valve|GXA_trunk_muscle|GXA_umbilical_cord|GXA_urinary_bladder|GXA_uterus|GXA_vagina|GXA_vas_deferens|GXA_venous_blood|GXA_visceral_(omentum)_adipose_tissue|GXA_whole_blood|GO|CADD_PHRED|CADD_RAW|miRNA|ExAC_AF|ExAC_AF_AFR|ExAC_AF_AMR|ExAC_AF_Adj|ExAC_AF_CONSANGUINEOUS|ExAC_AF_EAS|ExAC_AF_FEMALE|ExAC_AF_FIN|ExAC_AF_MALE|ExAC_AF_NFE|ExAC_AF_OTH|ExAC_AF_POPMAX|ExAC_AF_SAS'
    raw_field_names = raw_field_names.split('|')
    raw_field_names = [x.replace("(", "").replace(")", "").replace("-", "_").replace(',','') for x in raw_field_names] # remove parenthesis, replace -
    return raw_field_names

field_name = fieldname_generator()


def annotation_generator(annotation_str):

    """
    :param annotation_str: e.g.:CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|
    retained_intron|||||||||||4136|1|HGNC|24023||||,A|upstream_gene_variant|MODIFIER|PLEKHN1|ENSG00000187583|
    Transcript|ENST00000480267|retained_intron|||||||||||4261|1|HGNC|25284||||
    :return: a reperated list: the length is number of variation
    """

    annotation_str = annotation_str.split("CSQ=")[-1]  # remove "csq="
    annotation_list = annotation_str.split(',') # each one may have multiply variants
    re = [ x.split('|') for x in annotation_list] # split each variant
    return re


def annotation_cql_generator(field_name, annotation_list):

    """
    Construct part of a CQL String of the format [{pair1, pair2, ...}, ..., {pair1, pair2, ...}]
    :param field_name: set field name, from the parser fieldname_generator()
    :param annotation_list: list of values for various fields from one field in annotation_generator()
    :return: a string following CQL standard
    """
    re_str = "["
    for annotation in annotation_list:
        annotation_str = "{"
        pair = zip(field_name, annotation)
        for item in pair:
            item_sub_annotation = " {0}: '{1}',".format(item[0], item[1])
            annotation_str += item_sub_annotation # Construct the query string for each part
        annotation_str = annotation_str[:-1] + '}'
        re_str += annotation_str + ','
    re_str = re_str[:-1] + ']'
    return re_str


def vcf_byline_insert(raw_line):

    """
    :param raw_line: single line in VEP file
    :return: No return.
    Will be called in asynchronizing session.
    """
    if not raw_line.startswith("#"):
        # each worker should have its own session, see
        # http://stackoverflow.com/questions/37942249/cassandra-multiprocessing-cant-pickle-thread-lock-objects.
        cluster = Cluster(contact_points=contact_point_DB)
        db_session = cluster.connect()
        db_session.set_keyspace(keyspace_DB)

        line = raw_line.rstrip()
        annotation_list = line.split('\t')
        chrom = annotation_list[0]
        pos = annotation_list[1] # omit the 3rd one
        ref = annotation_list[3]
        alt = annotation_list[4]
        sub_annotation_list = annotation_generator(annotation_list[7])
        annotation_str = annotation_cql_generator(field_name, sub_annotation_list)
        db_insert((chrom, long(pos), ref, alt), annotation_str, db_session)
        cluster.shutdown()

def db_insert(key_content, insert_content, db_session):

    """
    :param key_content: the keys for the database: (chrom, long(pos), ref, alt)
    :param insert_content: the VEP annotator
    :param db_session: the database connection session
    :return: No return value
    """
    insert_statement = db_session.prepare(
        "INSERT INTO " + table_DB +
        " (chrom, pos, ref, alt, annotations) VALUES" +
        " (?, ?, ?, ?, " + insert_content + ")"
    )
    query = insert_statement.bind(key_content)
    db_session.execute(query)




if __name__ == "__main__":
    # getting arguments
    args = getargs()
    contact_point_DB = [args.contact_point]
    input_filename = args.input_file
    batch_size = int(args.processing_number)

    start = time.time()

    f = open(input_filename, 'rb')
    pool = Pool(batch_size)
    pool.map(vcf_byline_insert, f, batch_size)

    pool.close()
    pool.join()

    end = time.time()
    print "Time Used: ", (end - start)
