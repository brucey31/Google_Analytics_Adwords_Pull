__author__ = 'Bruce Pannaman'

import requests
from datetime import date, timedelta
import csv
import configparser
from subprocess import call, check_output
import os
import psycopg2

config = configparser.ConfigParser()
ini = config.read('conf2.ini')

AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')
ADJUST_KEY = config.get('Adjust Keys', 'key')

# Dict of app codes, in order of site
apps = ['yhsi1pxex728', 'jtedsr9c48w0', 'ikg39qjv27ls', 'je9jel72hlaq', 'w93p2l7297wl', 'ugcgvwx8dnll', 'zv2868fxr4xm', 'mtrbgjmnptxb', 'xwvln5xcy2vf', 'n2p3sskujhxm', 'vyk94lbxnrgg', 'xqcgkedmdf7j', 'epd84c76alvx', 'crcz7dcg9kgt', 'p3uz2gym79lz', 'bnzc4e7w56eb', 'b5xdygcr6z2p']
start_date = date(2016, 1, 1)
end_date = date.today() - timedelta(days=1)
# end_date = date(2016, 1, 6)


while start_date <= end_date:
    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Adjust_data/%s" % start_date.strftime("%Y-%m-%d")])

    if len(rs) > 1:
        print 'We have data for %s, moving on' % start_date

    else:
        with open('Adjust_Data_%s.csv' % start_date.strftime("%Y-%m-%d"), 'wb') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            for app in apps:
                response = requests.get("http://api.adjust.com/kpis/v1/%s?start_date=%s&end_date=%s&kpis=impressions,clicks,installs,revenue,revenue_events&sandbox=false&user_token=%s" % (app, start_date, start_date, ADJUST_KEY))
                response_json = response.json()

                app = response_json['result_set']['name'].replace('[', '').replace(']','')

                if 'trackers' in response_json['result_parameters']:

                    trackers = response_json['result_parameters']['trackers']
                    kpis = response_json['result_parameters']['kpis']
                    tracker_counter = 0

                    print '\n' + app + '\n'

                    for tracker in trackers:
                        value_counter = 0
                        tracker_false = tracker['has_subtrackers']

                        if tracker_false == True:

                            channel = tracker['name']
                            values = response_json['result_set']['trackers'][tracker_counter]['kpi_values']

                            for value in values:
                                list = []
                                metric = kpis[value_counter]
                                list.append(start_date.strftime("%Y-%m-%d"))
                                list.append(app)
                                list.append(channel)
                                list.append(metric)
                                list.append(value)

                                spamwriter.writerow(list)

                                print str(start_date) + ',' + app + ',' + channel + ',' + metric + ',' + str(value)

                                value_counter = value_counter + 1

                        tracker_counter = tracker_counter + 1

                else:
                    print '\n' + app + '\n'
                    print "nothing to get for this app"

        print 'Uploading to %s data to s3' % start_date
        call(["s3cmd", "put", 'Adjust_Data_%s.csv' % start_date.strftime("%Y-%m-%d"), "s3://bibusuu/Adjust_data/%s/" % start_date.strftime("%Y-%m-%d")])

        print 'Removing Local File'
        os.remove('Adjust_Data_%s.csv' % start_date.strftime("%Y-%m-%d"))

    start_date = start_date + timedelta(days=1)

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table adjust2"
cursor.execute("drop table if exists adjust2;")
print "Creating new table \n Adjust2 "
cursor.execute( "create table adjust2 (date date, app varchar(50), channel varchar(50), metrics varchar(50), value float);")
print "Copying Adjust CSV data from S3 to  \n Adjust2 "
cursor.execute( "COPY adjust2  FROM 's3://bibusuu/Adjust_data'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))
print "Dropping Table old table \n Adjust"
cursor.execute("DROP TABLE if exists adjust;")
print "Renaming table  \n Adjust2 \nto \n Adjust "
cursor.execute("ALTER TABLE adjust2 RENAME TO adjust")

print 'Complete, enjoy your adjust data'

conn.commit()
conn.close()
