# Data Analysis using data from Stack Exchange (Hadoop, Pig, Hive, MapReduce) 
The following was performed on a cluster in Dataproc in Google Cloud Platform (GCP) which already has Hadoop, Pig and Hive.

### 1. Acquire the top 200,000 posts by viewcount (From Stack Exchange)
Run the following queries on https://data.stackexchange.com/stackoverflow/query/new and download the data as csv.

```
> select * from posts where posts.ViewCount > 100000 ORDER BY posts.ViewCount DESC

> select * from posts where posts.ViewCount <=100000 and posts.ViewCount >58000 ORDER BY posts.ViewCount DESC

> select * from posts where posts.ViewCount <=58000 and posts.ViewCount >42500 ORDER BY posts.ViewCount DESC

> select * from posts where posts.ViewCount <=42500 and posts.ViewCount >33000 ORDER BY posts.ViewCount DESC

> select top 9458* from posts where posts.ViewCount <=33000 and posts.ViewCount >30000 ORDER BY posts.ViewCount DESC
```

We need to run 5 queries as only a maximum of 50,000 rows can be downloaded in one csv. You can also use other queries to sort and download your data. Now you will have 4 to 5 csv which have 2,00,000 records in total.

### 2. Using Pig or MapReduce, extract, transform and load the data as applicable
I uploaded the five csv files into my cluster and then put it in hdfs using `hdfs -put command`.
Then run Pig. 
Register the piggybank jar to the use the CSVLoader function.

```
> REGISTER /usr/lib/pig/piggybank.jar

> DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
```

Then using the load command, I loaded all the csv files to pig. The following is the load command for the first csv.

```
> data_stack1  = LOAD 'QueryResults1.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE','SKIP_INPUT_HEADER') as (Id:chararray, PostTypeId:chararray, AcceptedAnswerId:chararray,	ParentId:chararray,	CreationDate:chararray,	DeletionDate:chararray,	Score:chararray, ViewCount:chararray,	Body:chararray,	OwnerUserId:chararray,	OwnerDisplayName:chararray,	LastEditorUserId:chararray, LastEditorDisplayName:chararray, LastEditDate:chararray,	LastActivityDate:chararray,	Title:chararray,	Tags:chararray,	AnswerCount:chararray, CommentCount:chararray,	FavoriteCount:chararray, ClosedDate:chararray, CommunityOwnedDate:chararray);
```

Combine all the loaded files to give a combined file of 2,00,000, then take only some required fields and filter the null values in OwnerUserId and OwnerDisplayName.

```
> combined_data = UNION data_stack1, data_stack2, data_stack3, data_stack4,data_stack5 ;

> d1 = FOREACH combined_data GENERATE Id, Score, ViewCount, Body, OwnerUserId, OwnerDisplayName, Title, Tags;

> filtered = FILTER d1 by ((OwnerUserId != '') AND (OwnerDisplayName != ''));
```

The body column has many special characters which make the data messy, so we replace all special characters with spaces using REPLACE function. Then we put the data into the hdfs into a folder called result. The exit pig using `quit` command.

```
> STORE A INTO 'result' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE');
```

The results are saved into parts, merge these parts and put it into a csv(Query.csv) using `hadoop fs -getmerge` command. 

### 3. Using Hive and/or MapReduce, get:

Run hive using `hive` command. Create an external table in hive (data_exch) and load the data into the table.

```
> create external table if not exists data_exch (Id int, Score int, ViewCount int,Body String, OwnerUserId int, OwnerDisplayName string, Title string, Tags string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

> load data local inpath 'Query.csv' overwrite into table data_exch;
```

#### I. The top 10 posts by score

Run the following query to get the top posts by score.

```
> select distinct score from data_exch order by score desc limit 10;
```

#### II. The top 10 users by post score

Create a table with aliases for columns and run the query on this table. 
```
> create table grouped_users_posts as select ownerUserId as a, Body as b,SUM(Score) as c from data_exch group by ownerUserId,Body;

> select a,c from grouped_users_posts order by c desc limit 10;
```

#### III. The number of distinct users, who used the word “Hadoop” in one of their posts

Run the following query to find the distinct number of users, who used the word “Hadoop” in one of their posts.

```
> select COUNT(DISTINCT OwnerUserId) from data_exch where lower(Body) like '%hadoop%';
```

Put the table with selected columns for Q.4 into the local using the following

```
> INSERT OVERWRITE LOCAL DIRECTORY '/home/kirthyodackal/tfidfdata' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
select OwnerUserId,Body from data_exch where OwnerUserId in (select OwnerId from grouped_users_data);
```
The result file will be stored in the path mentioned in the insert overwrite command. If more than one file, merge them into a single file after replacing the comma with spaces to be used in Q4. The mapreduce program needs the input file to be separated by space rather than comma. Move them into a folder in the local from hdfs.

```
> cd tfidfdata
> sed 's/,/ /g' 000000_0 > inputfile

> hadoop fs -mkdir /mappred
> hadoop fs -put inputfile /mappred
```

### 4. Using Mapreduce calculate the per-user TF-IDF (just submit the top 10 terms for each of the top 10 users from Query 3.II)

Implementation of TFIDF in Hadoop using Python will be in three phases using three mappers and three reducers. Use the following commands to run TF-IDF on the cluster.

```
> hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/kirthyodackal/MapperPhaseOne.py /home/kirthyodackal/ReducerPhaseOne.py -mapper "python MapperPhaseOne.py" -reducer "python ReducerPhaseOne.py" -input hdfs://cluster-3299-m/mapinput/inputfile -output hdfs://cluster-3299-m/mappred1

> hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/kirthyodackal/MapperPhaseTwo.py /home/kirthyodackal/ReducerPhaseTwo.py -mapper "python MapperPhaseTwo.py" -reducer "python ReducerPhaseTwo.py" -input hdfs://cluster-3299-m/mappred1/part-00000 hdfs://cluster-3299-m/mappred1/part-00001 hdfs://cluster-3299-m/mappred1/part-00002 hdfs://cluster-3299-m/mappred1/part-00003 hdfs://cluster-3299-m/mappred1/part-00004	 -output hdfs://cluster-3299-m/mappred2

> hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -file /home/kirthyodackal/MapperPhaseThree.py /home/kirthyodackal/ReducerPhaseThree.py -mapper "python MapperPhaseThree.py" -reducer "python ReducerPhaseThree.py" -input hdfs://cluster-3299-m/mappred2/part-00000 hdfs://cluster-3299-m/mappred2/part-00001 hdfs://cluster-3299-m/mappred2/part-00002 hdfs://cluster-3299-m/mappred2/part-00003 hdfs://cluster-3299-m/mappred2/part-00004	 -output hdfs://cluster-3299-m/mappredf
```

Use the `hadoop fs -getmerge` command to merge the output files into a single csv into the local (tfidfout.csv). Replace the spaces with and save this into another csv (tfidfout1.csv). Create an external table in hive and load the csv into the table.

```
> sed -e 's/\s/,/g' tfidfout.csv > tfidfout1.csv

> create external table if not exists TFIDF_data2 (Term String,Id int,tfidf float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

load data local inpath 'tfidfout1.csv' overwrite into table TFIDF_data2;
```

Run the following query to get the top 10 terms for each of the top 10 users from Query 3.II.

```
SELECT *
FROM (
SELECT ROW_NUMBER()
OVER(PARTITION BY Id
ORDER BY tfidf DESC) AS TfidfRank, *
FROM TfIDF_data2) n
WHERE TfidfRank IN (1,2,3,4,5,6,7,8,9,10);
```

The mapreduce programs are from https://github.com/SatishUC15/TFIDF-HadoopMapReduce#tfidf-hadoop with minor changes (i.e. addition of stop words and the related `if` condition in MapperPhaseOne.py)
