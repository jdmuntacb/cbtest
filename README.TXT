README.TXT

A simple, flexible handy test Couchbase tool
   - Manage scopes, collections
   - Load and drop documents

Flexible:
 o Add a new method with Properties object as parameter in the main class or any other java class (uses java reflection)
 o Simply run as below with set of methods to execute:
   -Drun=<method>,<method>

Usage:

$ mvn clean package


To create 1000 documents in default bucket, default scope, default collection (default._default._default):
$ java -Durl=172.23.136.109 -Duser=Administrator -Dpassword=password -Drun=connectCluster,createDocs -Ddoc.count=1000  -Doperation=create -jar target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar

To create 10 scopes (project_1,..project_10) of bucket (db):
$ java -Durl=172.23.136.109 -Duser=Administrator -Dpassword=password -Drun=connectCluster,createScopes -Dbucket=db -Dscope=project -Dscope.count=10 -Doperation=create -jar target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar

To create 100 tenants (tenant_1,tenant_2..tenant_100) in each of 10 scopes (project_1,..project_10) of bucket (db):
$ java -Durl=172.23.136.109 -Duser=Administrator -Dpassword=password -Drun=connectCluster,createCollections -Dbucket=db -Dscope=project -Dscope.count=10 -Dcollection=tenant -Dcollection.count=100 -Doperation=create -jar target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar

To create 1000 documents in each of 100 tenants (tenant_1,tenant_2..tenant_100) in each of 10 scopes (project_1,..project_10) of bucket (db):
$ java -Durl=172.23.136.109 -Duser=Administrator -Dpassword=password -Drun=connectCluster,createTenantDocs -Dbucket=db -Dscope=project -Dscope.count=10 -Ddoc.count=1000 -Dcollection=tenant -Dcollection.count=100 -Doperation=create -jar target/cbtest-0.0.1-SNAPSHOT-jar-with-dependencies.jar

To delete, use the below in the above commands. To drop all collections, use -Doperation=dropall
-Doperation=drop
