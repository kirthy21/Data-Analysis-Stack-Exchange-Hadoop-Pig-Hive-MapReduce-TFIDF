# Data Analysis using data from Stack Exchange (Hadoop, Pig, Hive, MapReduce) 
The following was preformed on a cluster in Dataproc in Google Cloud Platform (GCP) which already has Hadoop, Pig and Hive. For mapreduce I used the files from https://github.com/SatishUC15/TFIDF-HadoopMapReduce#tfidf-hadoop with minor changes.

### 1. Acquire the top 200,000 posts by viewcount (From Stack Exchange)
Run the following queries on https://data.stackexchange.com/stackoverflow/query/new and download the data as csv.

> select * from posts where posts.ViewCount > 100000 ORDER BY posts.ViewCount DESC

> select * from posts where posts.ViewCount <=100000 and posts.ViewCount >58000 ORDER BY posts.ViewCount DESC

> select * from posts where posts.ViewCount <=58000 and posts.ViewCount >42500 ORDER BY posts.ViewCount DESC

> select * from posts where posts.ViewCount <=42500 and posts.ViewCount >33000 ORDER BY posts.ViewCount DESC

> select top 9458* from posts where posts.ViewCount <=33000 and posts.ViewCount >30000 ORDER BY posts.ViewCount DESC

We need to run 5 queries as only a maximum of 50,000 rows can be downloaded in one csv. You can also use other queries to sort and download your data. Now you will have 4 to 5 csv which have 2,00,000 records in total.

### 2. Using Pig or MapReduce, extract, transform and load the data as applicable
I uploaded the five csv files into my cluster and then put it in hdfs using hdfs -put command.
Then run Pig. 
Register the piggybank jar to the use the CSVLoader function.

> REGISTER /usr/lib/pig/piggybank.jar

> DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

Then using the load command, I loaded all the csv files to pig. The following is the load command for the first csv.

> data_stack1  = LOAD 'QueryResults1.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray,	DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);

Combine all the loaded files to give a combined file of 2,00,000, then take only some required files and filter the null values in OwnerUserId and OwnerDisplayName.

> combined_data = UNION data_stack1, data_stack2, data_stack3, data_stack4,data_stack5 ;

> d1 = FOREACH combined_data GENERATE Id, Score, ViewCount, Body, OwnerUserId, OwnerDisplayName, Title, Tags;

> filtered = FILTER d1 by ((OwnerUserId != '') AND (OwnerDisplayName != ''));


### 3. Using Hive and/or MapReduce, get:
#### I. The top 10 posts by score
#### II. The top 10 users by post score
#### III. The number of distinct users, who used the word “Hadoop” in one of their posts
### 4. Using Mapreduce calculate the per-user TF-IDF (just submit the top 10 terms for each of the top 10 users from Query 3.II)
