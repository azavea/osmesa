## S3-backed HBase performance

The standard HBase deployment involves a cluster of machines storing and
querying against HDFS which will sometimes mean that data locality is
taken advantage of (the blocks needed by an HBase region-server may well
live on that region-server's filesystem). On S3, the problem is
different: data locality isn't something that we can *ever* expect to
reliably take advantage of. This means that tuning caching and cluster
behavior is a more important piece of the puzzle for achieving acceptable
HBase performance.

Below is a collection of performance considerations relevant to working
with an S3-backed HBase cluster.


### Making consistency less 'eventual'

S3 consistency is sometimes very slow. To improve this situation,
provisioning EMRFS to take advantage of a DynamoDB table that tracks
metadata (this table can be shared between different clusters if using
the read-replica strategy). When 'consistent view' is on, S3 guarantees
read-after-write and list consistency.

[A blog](http://www.stackdriver.com/eventual-consistency-really-eventual/)
describing extremely consistency.

[This Netflix tech blog](https://www.infoq.com/news/2015/01/emrfs-s3-consistency)
about consistency (and its lack) on S3. Their solution, s3mper, (from
the latin for 'always') is the Netflix-rolled solution to this problem
(which also uses DynamoDB) likely informed the AWS 'consistent view'
feature.

[The AWS guide](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-5.1.0/emrfs-configure-consistent-view.html)
to configure consistent view on EMRFS.


### Caching


### FS-dependent chores
The [HFileCleaner](https://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/master/cleaner/HFileCleaner.html)
class, a [Chore](https://hbase.apache.org/0.94/apidocs/org/apache/hadoop/hbase/Chore.html)
performed by HBase every 5 minutes by default can take more than 5
minutes on large datasets when HBase is backed by S3. Consequently, the
FINRA HBase deployment described [here](https://aws.amazon.com/blogs/big-data/low-latency-access-on-trillions-of-records-finras-architecture-using-apache-hbase-on-amazon-emr-with-amazon-s3/)
actually disables automatic HFile cleaning. Instead, FINRA engineers opted to
"run it manually during a maintenance window."


### Dropping tables
AWS documentation suggests disabling rather than dropping tables due to
the poor performance which attends massive reads and writes to S3 (as
opposed to deployments which can assume far greater data locality). The
writeup of the FINRA deployment described [here](https://aws.amazon.com/blogs/big-data/low-latency-access-on-trillions-of-records-finras-architecture-using-apache-hbase-on-amazon-emr-with-amazon-s3/)
mentions needing to increase timeout settings for "management operations
related to dropping a table."

