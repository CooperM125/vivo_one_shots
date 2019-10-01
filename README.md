# vivo_one_shots
Quick programs for fixing VIVO issues
Python3
## Setup
create virtual enviroment

pip install request

pip install pyyaml

fill out feilds in config.yaml

## Person Audits
Tools for cleaning up Persons in VIVO

### clean_persons_dupe_authorships
This tool is to be used on a Person who is linked multiple times to the same Article i.e. they have multiple Authorships with one Article.

## Pub Audits
Tools for cleaning up Articles in VIVO

### clean_author_bombs
This tool is for Articles that have been mistakenly matched with huge numbers on authors. A sub file is created that delinks all authors from the publication. It will also delete Persons that have no other publications.

### clean_ghost_authors
This tool is used when an Article has duplicated authors that are not a result of duplicate Authorships. These are two separate profiles for the same person, both linked to the publication. The tool groups authors by names and removes duplicates. Persons that have no other publications are assumed to be a stub and are deleted.

### clean_pubs_dupe_authorship
This tool is for when an Article is linked multiple times to a single Person i.e. they have multiple Authorships with one Person.

# MultiShot
Tool that runs through oneshots and corrects data problems in vivo.

Current supported query oneshot combos
<li> pubs_dupe_authorships - cleans duplicate authorships</li>
<li> more coming soon!</li>

## Running 
The command to run all oneshots is:

## Development
Writing a oneshot for multishot there must be at least one of the two functiuns.
<li> get_add_trips - returns triples that need to be added into vivo given uri.</li>
<li> get_sub_trips - returns triples that need to be removed into vivo given uri.</li>

For more detail please look at the oneshot.example file. 
