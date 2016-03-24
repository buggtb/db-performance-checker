# db-performance-checker

Build to compare query execution times across 2 different databases. 
Could do with a lot of TLC to make it any good, but does the job.

java -jar ./target/db-performance-checker-1.0-SNAPSHOT-jar-with-dependencies.jar -config ./config-file -querypath ./path/to/queries/ -concurrency 1 -validateresult

config file is mandatory and sample exists in files folder.

path to queries is also mandatory, again samples exist in files folder

concurrency can either be int for number of queries run at once or "all" for everything

validateresult optional flag, will attempt to compare resultsets to ensure the same data was returned YMMV
