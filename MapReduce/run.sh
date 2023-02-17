export MR_OUTPUT=/user/root/output-data

hadoop fs -rm -r $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Simple streaming job reduce' \
-Dmapred.reduce.tasks=1 \
-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
-input s3a://a-tagirov/ny-taxi/ -output $MR_OUTPUT
