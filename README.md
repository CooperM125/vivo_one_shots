# vivo_one_shots
Quick programs for fixing VIVO issues

## Cleaning up authors

### clean_duplicate_authorships
This tool is used for when single instances of an author have been listed multiple times on an article.

### clean_ghost_authors
This tool is used when an author has duplicate profiles that are listed on the same article. If the duplicates have no other publications, this tool will also remove those.
If an article has duplicate authorships and ghost authors, clean the authorships first.



==== Ignore Below ====


### clean_article_author_dupes
This tool is used when an author has been listed multiple times on a single article. One instance of the author will be left and all repeats will be cleared out.

### clean_cluster_authors
This tool is used to find and erase authors that have overly long labels (suggesting it is a list of authors rather than an individual). The cutoff is a label of over 100 characters, but this can be changed by editing the sparql query in the code.

## Fixing bad strings
`json_string_fixer` and `csv_string_fixer` differ on more than just the type of input they take. The csv fixer fixes a much broader set of bad strings. And while you can get the input file for the json fixer using Sparql, the csv fixer was designed using a csv produced by the underlying sql server (although you can use Sparql if you do not need to reach the depth of access that the sql server gives you).

### json_string_fixer
To produce the json file, the query you use must have ?s for the subject and ?o for the object. You will need to write what predicate you searched for in the program itself (and thus they must all have the same predicate). Most often, it will be `<http://www.w3.org/2000/01/rdf-schema#label>`. Here is an example query:

```
SELECT ?s ?o
WHERE {
    ?s <http://www.w3.org/2000/01/rdf-schema#label> ?o .
    ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Journal> .
}
```

This query will get the labels for everything that is type bibo:Journal. Strings that are properly typed will have a datatype or an xml:lang on the object. These are filtered out by the tool to collect all of the improperly typed strings.

### csv_string_fixer
This string fixer involves producing a csv from the sql server. The queries you will need to run are:
```
CREATE temporary table if not exists temp_tbl as
(SELECT q.s, ns.lex as s_lex, q.p, np.lex as p_lex, q.o, no.lex as o_lex
FROM Quads as q
JOIN Nodes as ns ON q.s=ns.hash
JOIN Nodes as np ON q.p=np.hash
JOIN Nodes as no ON q.o=no.hash
WHERE q.g='-364693509095697557');
```

```
SELECT * FROM temp_tbl as t 
WHERE t.o in
(SELECT n.hash FROM Nodes as n 
WHERE lang='' AND datatype='' AND type!='2' AND type!='1')
INTO OUTFILE 'file.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '|';
```

The file produced by this is what you will use with the string fixer.