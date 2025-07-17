/* whether the PK is clustered or not doesn't matter in this case */
CREATE TABLE nonclustered_cache1_initial_autoid (
   id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
   v int,
   PRIMARY KEY (id) NONCLUSTERED
) AUTO_ID_CACHE=1 AUTO_INCREMENT = 100;
