# MapReduce on Twitter dataset
This repository contains three MapReduce codes using Apache Hadoop.  
There are two types of codes, case sensitive hashtags and case insensitive hashtags.  
- First code (Q1.java) counts the likes and retweets of tweets that contain #Trump or #DonaldTrump and also counts tweets that contain #Biden or #JoeBiden.  
- Second code (Q2.java) shows what percentage of a countries tweets contain Trump tweets, Biden tweets, or both and also shows the number of tweets that contain any of these hashtags. It uses the country field to determine the tweet location.  
- Third code (Q3.java) shows how what percentage of a countries tweets similar to second code, but uses the altitude and latitude fields to calculate the user whereabouts.  
The outputs are in the output directories.  

# Run it
Here are some useful links to help you run the MapReduce codes on Apache Hadoop:  
[How to setup Hadoop Multi node cluster on Ubuntu](https://pnunofrancog.medium.com/how-to-set-up-hadoop-3-2-1-multi-node-cluster-on-ubuntu-20-04-inclusive-terminology-2dc17b1bff19 "How to setup Hadoop Multi node cluster on Ubuntu")  
[MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html "MapReduce Tutorial")  
  
Also you can use the dataset provided in the repository.  
