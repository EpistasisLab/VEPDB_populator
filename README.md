## VEPDB_populator
####  Brian S. Cole PhD, Dichen Li MCIT, Zhengxuan Wu, and Yingjie Luan
##### Institute for Biomedical Informatics, University of Pennsylvania Perelman School of Medicine, Philadelphia PA

This directory contains Python source to populate a Cassandra database with genetic variant effect annotations from ENSEMBL Variant Effect Predictor (VEP) output in VCF format (VEP VCF), in which annotation information are stored as CSQ strings.

db_populator.py: parallel Cassandra database population built on the Datastax Python driver and the multiprocessing library for parallel execution.  Input is a VEP VCF file.

### An example input line (only the INFO column is displayed):

#### CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|retained_intron|||||||||||4136|1|HGNC|24023|1|2|3|4

Multiple comma-separated annotations are all handled separately and a collection of the annotations is built.

### An example input line with multiple annotations:
#### 1  901994  G       A       CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|retained_intron|||||||||||4136|1|HGNC|24023||||,A|upstream_gene_variant|MODIFIER|PLEKHN1|ENSG00000187583|Transcript|ENST00000480267|retained_intron|||||||||||4261|1|HGNC|25284||||

Multiple annotations can arise from multiple genes (as shown in this example: KLHL17 and PLEKHN1 are separate, comma-separated annotations), multiple isoforms of the same gene, or polyallelic variants (two or more ALT alleles) which may be layered additionally on top of multiple genes/isoforms.

##Note:

Running this script requires a running Cassandra node or cluster as one or more contact points (IP addresses).  This means the Cassandra node/cluster must be running (nodetool status reports UN status "up-normal"), configured to accept connections over the ports Cassandra requires (9042/9160 e.g.), and with the keyspace, table, and user-defined type already declared.  We provide a CQL script, create_table.cql, which you can source from the CQLSH on a running Cassandra node to automate the creation of the vepdb_keyspace, the vepdb table, and the annotation user-defined type.

