

from urllib2 import Request, urlopen
import json
import re
import boto.s3
import dateutil.parser
import pandas as pd


FIELD_NAME = 'call_count'
DB_OPS = ['select', 'update', 'insert', 'delete']
BASE_URL = 'https://api.newrelic.com/api/v1/{0}/{1}/{2}.json'


class NewRelic:
    """"NewRelic API"""

    def __init__(self, config):
        self.config = config
        self.api_key = config['api_key']
        self.account_id = config['account_id']
        self.app_names = config['apps']
        self.headers = {'x-api-key': self.api_key}
        self.local_cache_dir = config['local_cache_dir']

        self.s3_conn = boto.connect_s3(aws_access_key_id=config['aws_key'],
                                      aws_secret_access_key=config['aws_secret_key'])
        self.s3_bucket = self.s3_conn.get_bucket(config['s3_bucket'])
        self.apps = {}

    def cacheable(func):
        def cache_checker(*args, **kwargs):
            self = args[0]
            cache_key = '_'.join(str(i) for i in args[1:])
            local_cache_file ='{}/{}'.format(self.local_cache_dir, cache_key)

            #print "Checking cache for {}, {}".format(args,kwargs)
            try:
                #print "reading file: {}".format(local_cache_file)
                result = "{}"
                with open(local_cache_file, 'r') as f: 
                    return json.load(f)

#result = "".join(f.readlines())
 #               return json.load(result)
            except IOError:
                pass

            #print "Checking s3 for {}. {}".format(args,kwargs)
            s3_cache_key = self.s3_bucket.get_key(cache_key)
            if s3_cache_key:
                s3_cache_value = s3_cache_key.get_contents_as_string()
                #print " returning s3 file"
                s3_cache_value_json = json.loads(s3_cache_value)
                self._write_local_cache(local_cache_file, s3_cache_value_json)
        
            return_value = func(*args, **kwargs)

            key = self.s3_bucket.new_key(cache_key)
            key.set_contents_from_string(json.dumps(return_value))

            self._write_local_cache(local_cache_file, return_value)
                        
            return return_value
        return cache_checker

    def _write_local_cache(self, file_name, value):
        with open(file_name, 'w') as f:
            json.dump(value, f)


    def fetch_db_data(self, start_date, end_date):
        self._load_application_ids();
        data = {}
        for app_id in self.apps.keys():
            result = self._fetch_app_data(app_id, 
                                          'Database', 
                                          start_date, 
                                          end_date)

            data[self.apps[app_id]] = result
        return data

    
    def fetch_db_data_dataframe(self, start_date, end_date):
        data_for_frame = []
        data = self.fetch_db_data(start_date, end_date)
        
        for app, stats in data.items():
            raw_data = stats['raw_data']

            for table, stats in raw_data.items():
                for data_point in stats:                    
                    cleaned_data = {}

                    m = re.search(r"Database/(.*)/(.*)", data_point['name'])            
                    if not m:
                        pass
                    else:
                        cleaned_data['table'] = m.groups()[0]
                        cleaned_data['op'] = m.groups()[1]
                        if m.groups()[1] == "select": 
                            cleaned_data['type'] = 'read'
                        else:
                            cleaned_data['type'] = 'write'

                    cleaned_data['begin'] = dateutil.parser.parse(data_point['begin'])
                    cleaned_data['end'] = dateutil.parser.parse(data_point['end'])
                    cleaned_data['app'] = data_point['app']
                    cleaned_data['count'] = data_point['call_count']

                    # [{'begin': u'2013-11-10T22:00:00Z', 
                    #   'end': u'2013-11-10T22:06:00Z', 
                    #   'name': 'Database/sometable/select',
                    #   'app': 'APPNAME', 
                    #   'agent_id': 12343, 
                    #   'call_count': 711.6},
                    data_for_frame.append(cleaned_data)

        

        return pd.DataFrame(data_for_frame)


    def _load_application_ids(self):
        if self.apps:
            return

        r = Request(BASE_URL.format('accounts', self.account_id, 'applications'),
                    headers=self.headers)
        app_data = json.loads(urlopen(r).read())
        
        self.apps = {}
        for application in app_data:
            if application['name'] in self.app_names:
                self.apps[application['id']] = application['name']

    def _fetch_metrics_for_app(self, app_id):
        r = Request(BASE_URL.format('applications', app_id, 'metrics'),
                    headers=self.headers)
        return json.loads(urlopen(r).read())

    def _parse_table_operations(self, metrics):
        table_operations = {}
        for metric in metrics:
            m = re.search(r"Database/(.*)/(.*)", metric['name'])
            if m:
                table = m.groups()[0]
                operation = m.groups()[1]
                if table not in table_operations:
                    table_operations[table] = [operation]
                else:
                    table_operations[table].append(operation)
        return table_operations

    @cacheable
    def _fetch_app_data(self,app_id, metric_type, start_date, end_date):
        print 'Fetching data for {0}'.format(self.apps[app_id])
        
        # metrics looks like this:
        # [{'fields': ['average_exclusive_time', 
        #               'average_response_time', 
        #               'average_value', 
        #               'call_count', 
        #               'calls_per_minute', 
        #               'max_response_time', 
        #               'min_response_time'], 
        #  'name': 'Agent/MetricsReported/count'}, ....
        metrics          = self._fetch_metrics_for_app(app_id)

        # table_operations looks like this:
        # {'tabl1': ['select'], 
        #  'table2': ['select'],
        #  'table3': ['insert', 'select', 'delete']
        table_operations = self._parse_table_operations(metrics)
                      
        print ' - {0} tables'.format(len(table_operations.keys()))

        app_data = {'app_id' : app_id, 'metric_type': metric_type, 'start_date' : start_date, 'end_date' : end_date, 'raw_data' : {}}

        for table, operations in table_operations.items():
            table_data = self._fetch_details(app_id,table,operations,metric_type,start_date,end_date)
            app_data['raw_data'][table] = table_data

#        app_data['raw_data'] = json.dumps(app_data['raw_data'])
#        print " raw data length: {}".format(len(app_data['raw_data']))
        return app_data

    def _fetch_details(self,app_id,table,operations,metric_type,start_date,end_date):
        metric_params = []
        for operation in operations:
            metric_name = 'metrics[]={}/{}/{}'.format(metric_type,table, operation)
            metric_params.append(metric_name)
        
        all_metrics = '&'.join(metric_params)
        #print ' - fetching data for {0}'.format(table)
        metric_url = (BASE_URL.format('applications', app_id, 'data') +
                    '?begin={0}&end={1}&{2}&field={3}'.format(
                        start_date, end_date, all_metrics, FIELD_NAME))
        
        #print 'using url: {0}'.format(metric_url)
        r = Request(metric_url, headers=self.headers)

        # metric_data looks like this:
        # [{'begin': u'2013-11-10T22:00:00Z', 
        #   'end': u'2013-11-10T22:06:00Z', 
        #   'name': 'Database/some_table/select',
        #   'app': 'APPNAME', 
        #   'agent_id': 12345, 
        #   'call_count': 711.6},
        return json.loads(urlopen(r).read())
#        return metric_data





