## VEPDB_populator
####  Brian S. Cole PhD, Dichen Li MCIT, and Zhengxuan Wu
##### Institute for Biomedical Informatics, University of Pennsylvania Perelman School of Medicine, Philadelphia PA

This directory contains Python source to populate a Cassandra database with genetic variant effect annotations from ENSEMBL Variant Effect Predictor (VEP).

populate_vep_cassandra.py: python script to populate cassandra database with sample data via CQLSH INSERT commands (iterative approach)

mainscript.py: Python script to generate a CSV file amenable to the CQLSH COPY command (e.g. from S3 storage)

### An example input line:

#### CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|retained_intron|||||||||||4136|1|HGNC|24023|1|2|3|4

Multiple comma-separated annotations are all handled separately and a collection of the annotations is built.  Here's an example of how that looks in Python:

### An example input line with multiple annotations:
#### 1  901994  G       A       CSQ=A|downstream_gene_variant|MODIFIER|KLHL17|ENSG00000187961|Transcript|ENST00000463212|retained_intron|||||||||||4136|1|HGNC|24023||||,A|upstream_gene_variant|MODIFIER|PLEKHN1|ENSG00000187583|Transcript|ENST00000480267|retained_intron|||||||||||4261|1|HGNC|25284||||

## Will be converted to (without line break or indent):
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

