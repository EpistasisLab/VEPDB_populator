import gzip
import re
import sys
from datetime import datetime
from threading import Thread
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

__author__ = 'dichenli'

# This is the VEP_DB populator. The schema of the DB is:
#   (chrom:text, pos:bigint, ref:text, alt:text, annotations: List<frozen<vep_annotation>>)
#   where vep_annotation is a user-defined data type in Cassandra with the following fields:
#   (vep: text, lof: text, lof_filter: text, lof_flags: text, lof_info: text, other_plugins: text)
# vep is the basic annotation string
# lof|lof_filter|lof_flags|lof_info are the fields from LoF plugin.
# other_plugins are other plugins that may be added. It can be empty.
# This script assumes LoF is the first plugin, all other plugins come after LoF in the input string
# A frozen list is like a tuple, it has fixed number of elements:
# https://docs.datastax.com/en/cql/3.3/cql/cql_reference/collection_type_r.html
#
# Currently this Python script only supports a single gzipped input file...
#
# Useful external documentation:
# https://cassandra.apache.org/doc/cql3/CQL-3.0.html
# https://github.com/datastax/spark-cassandra-connector/tree/master/doc

KEYSPACE = "dage"
LINE_TYPE = "vep_annotation"
TABLE = "vep_db"

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

# create KEYSPACE:
# https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html
session.execute(
    "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
    " WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east': 3}"
    #For multiple data centers, specify the replication factor of each one here.
)
session.set_keyspace(KEYSPACE)

# Each key (chromosome, position, ref, alt) maps to a list of annotations, each one
# has the basic VEP fields, and then any other plugin fields. This structure
# is represented by the type below:
session.execute(
    "CREATE TYPE IF NOT EXISTS " + LINE_TYPE+
    " (vep text, lof text, lof_filter text, lof_flags text, lof_info text, other_plugins text)"
)

# Compound key of (chromosome, position, ref, alt), the value is a list of the annotations.
session.execute(
    "CREATE TABLE IF NOT EXISTS " + TABLE +
    "(chrom text, pos bigint, ref text, alt text, annotations list<frozen<" + LINE_TYPE + ">>, " +
    "PRIMARY KEY  ((chrom, pos, ref, alt)))" 
    #TODO: In the next version, we should use "PRIMARY KEY ((chrom), pos, ref, alt)"
    #so that we have both partition key and clustering keys inside primary key.
    #See https://jagadeeshs.wordpress.com/2015/07/15/cassandra/
)


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
    matched = re.search('^((.*?\|){22}?)(.*?)\|(.*?)\|(.*?)\|(.*?)(\|.*)?$', annotation)
    if matched is None:
        return None  # wrong formatted line
    vep = matched.group(1)
    lof = matched.group(3)
    lof_filter = matched.group(4)
    lof_flags = matched.group(5)
    lof_info = matched.group(6)
    others = matched.group(7)
    if others is None:
        others = ''
    return "{{vep: '{0}', lof: '{1}', lof_filter: '{2}', lof_flags: '{3}', lof_info: '{4}', other_plugins: '{5}'}}" \
        .format(vep, lof, lof_filter, lof_flags, lof_info, others)


def parse_line(raw_line):
    """Parse a line of raw data, convert to a tuple of 5 fields to present the 5 fields of cassandra DB.
    The first 4 fields are  chromosome, position, ref, alt. The last field (a string) represents
    the list of annotations compatible with CQL syntax.

    For example, an input:
    1	901994	G	A	CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|
    retained_intron|||||||||||4136|1|HGNC|24023||||,A|upstream_gene_variant|MODIFIER|PLEKHN1|ENSG00000187583|
    Transcript|ENST00000480267|retained_intron|||||||||||4261|1|HGNC|25284||||

    Will be converted to (without line break or indent):
    (1, 901994, 'G', 'A',
        "[{
            vep: 'CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|
            retained_intron|||||||||||4136|1|HGNC|24023|',
            lof: '', lof_filter: '', lof_flags: '', lof_info: '', others: ''
        }, {
            vep: 'A|upstream_gene_variant|MODIFIER|PLEKHN1|ENSG00000187583|Transcript|ENST00000480267|
            retained_intron|||||||||||4261|1|HGNC|25284|',
            lof: '', lof_filter: '', lof_flags: '', lof_info: '', others: ''
        }]"
    )
    """
    annotation_list = raw_line.split('\t')
    chrom = annotation_list[0]
    pos = annotation_list[1]
    ref = annotation_list[2]
    alt = annotation_list[3]
    annotations_str_list = annotation_list[4].split(',')
    annotations = map(match_annotation, annotations_str_list)
    if annotations.__contains__(None):
        return None  # wrong formatted line
    # return a tuple
    return chrom, long(pos), ref, alt, "[" + ", ".join(annotations) + "]"


def insert(raw_line, db_session):
    """Parse a raw line to compose CQL query and execute it to insert the line"""
    parsed = parse_line(raw_line)
    if parsed is None:
        return False
    insert_statement = db_session.prepare(
        "INSERT INTO " + TABLE +
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
