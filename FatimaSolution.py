from pyspark import SparkContext
import csv
import StringIO
from dateutil.parser import parse
import datetime
from pyspark.sql import SQLContext, Row 
import pandas
from subprocess import call
import os.path

# unzip the given data file

file_name = "data/2015_07_22_mktplace_shop_web_log_sample.log" 
if not os.path.isfile(file_name):
    call(["gunzip", "-k", file_name + ".gz"])

# read in the data to an rdd

sc = SparkContext("local", "Paytm Challenge")
logData = sc.textFile(file_name).cache()

# get desired rdd from logData

def parse_log_line(log_line):
    columns = csv.reader(StringIO.StringIO(log_line), delimiter=' ').next() 
    record = {}
    record['ip'] = columns[2].split(':')[0]
    record['timestamp'] = parse(columns[0])
    record['url'] = columns[11].split(' ')[1] 
    return record

rdd_relevant = logData.map(parse_log_line)

rdd_relevant_ordered = rdd_relevant.groupBy(lambda record: record['ip'])\
                                   .mapValues(list).mapValues(lambda v: sorted(v, key=lambda r: r['timestamp']))
 
# find sessions within each ip group

inactivity_window = 15  # minutes of inactivity needed to separate sessions

def add_session(list_of_records):
    previous_record = list_of_records[0]
    i = 0
    for record in list_of_records:
        if (record['timestamp'] - previous_record['timestamp']) >= datetime.timedelta(minutes=inactivity_window):
            i += 1
        record['session'] = i
        previous_record = record
    return list_of_records

rdd_by_session = rdd_relevant_ordered.mapValues(add_session)\
                                     .flatMapValues(lambda x:x).values()

# find start and end times of sessions and unique urls for each session

def agg_comb_fcn(x, y):
    x['max_time'] = max(x['max_time'], y['max_time'])
    x['min_time'] = min(x['min_time'], y['min_time'])
    x['url_set'] = x['url_set'].union(y['url_set'])
    return x

def agg_seq_fcn(x, rec):
    x['max_time'] = max(x['max_time'], rec['timestamp']) if x['max_time'] else rec['timestamp']
    x['min_time'] = min(x['min_time'], rec['timestamp']) if x['min_time'] else rec['timestamp']
    x['url_set'].add(rec['url'])
    if not x['ip']:
        x['ip'] = rec['ip']
    if not x['session']:
        x['session'] = rec['session']
    return x

zero_value = {'max_time': None, 'min_time': None, 'url_set': set(), 'ip': None, 'session': None}

rdd_times_and_urls = rdd_by_session.keyBy(lambda record: (record['ip'], record['session']))\
                                   .aggregateByKey(zero_value, agg_seq_fcn, agg_comb_fcn)\
                                   .values()

# find session times and number of unique urls per session

def session_duration_fcn(x):
    x['duration'] = (x['max_time'] - x['min_time']).total_seconds()
    x['url_count'] = len(x['url_set'])
    x['url_set'] = list(x['url_set'])  # this is to help when printing to json later
    return x

rdd_durations_and_url_counts = rdd_times_and_urls.map(session_duration_fcn)
rdd_durations_and_url_counts.cache()

# find average session time

avg_session_time = rdd_durations_and_url_counts.map(lambda x: x['duration'])\
                                               .mean()

# find most engaged ips

num = 50  # number of sessions to print out

most_engaged = rdd_durations_and_url_counts.map(lambda x: {i:x[i] for i in ['duration', 'ip', 'session']})\
                                           .top(num, lambda x: x['duration'])

# print out relevant rdds to files as json and relevant info to screen
# uncomment the commented lines to print out the sessionized log and
# durations and url counts, but keep in mind these files will be large

# sessionized, durations = "results/SessionizedLog.json", "results/DurationsAndUrlCounts.json"
engaged = "results/MostEngaged.json"
sqlContext = SQLContext(sc)
# sqlContext.createDataFrame(rdd_by_session.map(lambda record: Row(**record))).toPandas().to_json(sessionized, orient='records')
# sqlContext.createDataFrame(rdd_durations_and_url_counts.map(lambda record: Row(**record))).toPandas().to_json(durations, orient='records')
sqlContext.createDataFrame([Row(**record) for record in most_engaged]).toPandas().to_json(engaged, orient='records')
# print "The sessionized log has been written to %s" % sessionized
# print "The session durations and unique URL counts have been written to %s" % durations
print "The average session time is: %f seconds" % avg_session_time
print "The top %i IPs and sessions sorted by duration have been written to %s" % (num, engaged)
