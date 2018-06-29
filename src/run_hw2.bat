ssh root@localhost -p 2222 command pip install user-agents
ssh root@localhost -p 2222 command mkdir /usr/jobs/auto_test/
ssh root@localhost -p 2222 command mkdir /usr/jobs/auto_test/output/

scp -P 2222 bytes_counter_job.py root@localhost:/usr/jobs/auto_test/test.py
scp -P 2222 ..\input\http_input root@localhost:/usr/jobs/auto_test/test.txt

ssh root@localhost -p 2222 command hadoop fs -copyFromLocal /usr/jobs/auto_test/test.txt /user/raj_ops/test.txt

ssh root@localhost -p 2222 command "python3 /usr/jobs/auto_test/test.py -r hadoop hdfs:///user/raj_ops/test.txt --hadoop-streaming-jar=/usr/hdp/2.6.4.0-91/hadoop-mapreduce/hadoop-streaming.jar > /usr/jobs/auto_test/output.csv"


ssh root@localhost -p 2222 command hadoop fs -copyToLocal /user/raj_ops/output.csv /usr/jobs/auto_test/output.csv
scp -P 2222 root@localhost:/usr/jobs/auto_test/output.csv ..\output\output.csv

ssh root@localhost -p 2222 command rm -r /usr/jobs/auto_test
ssh root@localhost -p 2222 command hadoop fs -rm -skipTrash /user/raj_ops/test.txt
ssh root@localhost -p 2222 command hadoop fs -rm -r -skipTrash /user/raj_ops/output
ssh root@localhost -p 2222
