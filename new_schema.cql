CREATE KEYSPACE IF NOT EXISTS vepdb_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE vepdb_keyspace;

CREATE TYPE IF NOT EXISTS vep_annotation (
    Allele text,
    Consequence list<text>,
    IMPACT text,
    SYMBOL text,
    Gene text,
    Feature_type text,
    Feature text,
    BIOTYPE text,
    EXON text,
    INTRON text,
    HGVSc text,
    HGVSp text,
    cDNA_position text,
    CDS_position text,
    Protein_position text,
    Amino_acids text,
    Codons text,
    Existing_variation text,
    DISTANCE text,
    STRAND text,
    FLAGS text,
    SYMBOL_SOURCE text,
    HGNC_ID text,
    REFSEQ_MATCH text,
    GMAF text,
    CLIN_SIG text,
    SOMATIC text,
    PHENO text,
    GXA map<text, double>,
    GO set<text>,
    CADD_PHRED text,
    CADD_RAW text,
    miRNA text,
    ExAC map<text, double>
);

CREATE TABLE IF NOT EXISTS vepdb (
    chrom text, pos bigint, ref text, alt text, annotations list<frozen<vep_annotation>>,
    PRIMARY KEY ((chrom), pos, ref, alt)
);
