from .person_audits import clean_persons_dupe_authorships
from .pub_audits import clean_pubs_author_bombs, clean_pubs_dupe_authorships

PersonAudits = [
    clean_persons_dupe_authorships.CleanPersonsDuplicateAuthorships
]

PubAudits = [
    clean_pubs_author_bombs.CleanPubsAuthorBombs,
    clean_pubs_dupe_authorships.CleanPubsDupeAuthorships
]
