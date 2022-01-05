#!/usr/bin/env python3

from termcolor.color                          import Color
import sqlite3
import json
from datetime                           import datetime
from lib.sql                            import SQLBase
from lib.std                            import pre
from collections                        import OrderedDict

INFO = Color.fg('green')
CYAN = Color.fg('cyan')

def to_date(ts):
    try:
        return datetime.strptime(ts, "%A, %d. %B %Y %H:%M %Z")
    except ValueError:
        return datetime.strptime(ts, '"%A, %d. %B %Y %H:%M %Z"')

# MessageError
#
class MessageError(Exception):
    pass

# MissingRecordError
#
class MissingRecordError(Exception):
    pass

class BadIdQueryError(Exception):
    pass

class NoRecordsFoundError(Exception):
    pass

class TestStatsDB(SQLBase):

    def __init__(self):
        super(TestStatsDB, self).__init__('/home/work/ksc/messages-to-db/stats.sql3')
        self.__init_schema()

    def __init_schema(self):
        self.__init_schema_test_stats_table()

    def __init_schema_test_stats_table(self):
        try:
            cursor = self.sql.cursor()

            q  = 'create table if not exists '
            q += 'test_stats ( '
            q += '    id                    integer primary key,'    # A unique identifier
            q += '    sru_cycle             text,'               # The SRU cycle id (without the spin #)
            q += '    sru_cycle_raw         text,'               # SRU cycle id with spin #
            q += '    sru_cycle_spin        text,'               # SRU cycle spin

            q += '    kernel_source         text,'               # Kernel package name
            q += '    kernel_version        text,'               # Package version
            q += '    kernel_flavour        text,'               # Package flavour

            q += '    series_codename       text,'               # Xenial, Bionic, Focal, etc.

            q += '    total_tests           integer,'            # Total # of tests that have been created for this kernel
            q += '    tests_running         integer,'            # Number of tests currently running
            q += '    tests_passed          integer,'            # Number of tests that have passed
            q += '    tests_failed          integer,'            # Number of tests that have failed
            q += '    deploy_failed         integer'            # Number of tests that failed to deploy

            q += ');'

            cursor.execute(q)
            self.sql.commit()
        except Exception as e:
            self.sql.rollback()
            self.sql.close()
            raise e

    def update(self, msg):
        # cloud
        # kernel-flavour
        # kernel-source
        # kernel-version
        # key
        # op
        # original-key
        # original-op
        # series-codename
        # sru-cycle
        # sru-cycle-raw
        # sru-cycle-spin
        # timestamp
        # "arches" :
        #     <arch> :
        #         "instance-types" :
        #             <instance-type>
        #                 "tests" :
        #                     <test> : <job-name>
        q  = 'select * from test_stats where '
        q += f'kernel_source = "{msg["kernel-source"]}" '
        q += f'and kernel_version = "{msg["kernel-version"]}" '
        q += f'and series_codename = "{msg["series-codename"]}" '
        q += f'and sru_cycle_raw = "{msg["sru-cycle-raw"]}" '
        q += f'and kernel_flavour = "{msg["kernel-flavour"]}" '
        q += ';'
        try:
            found = self.fetch_one(q)
        except sqlite3.OperationalError:
            pre('Exception thrown executing:\n    %s\n' % q)
            pre(q)
            raise
        if found is not None:
            pre('found is not None')
            sru_cycle       = found['sru_cycle']
            sru_cycle_raw   = found['sru_cycle_raw']
            sru_cycle_spin  = found['sru_cycle_spin']
            kernel_source   = found['kernel_source']
            kernel_version  = found['kernel_version']
            kernel_flavour  = found['kernel_flavour']
            series_codename = found['series_codename']
            tests_running   = found['tests_running']
            tests_passed    = found['tests_passed']
            tests_failed    = found['tests_failed']
            deploy_failed   = found['deploy_failed']
            if msg['op'] == 'jobs.created':
                id = found['id']

                total_tests = found['total_tests']
                for arch in msg['arches']:
                    for instance_type in msg['arches'][arch]['instance-types']:
                        total_tests += len(msg['arches'][arch]['instance-types'][instance_type]['tests'])

        else:
            pre('found is None')
            sru_cycle       = msg['sru-cycle']
            sru_cycle_raw   = msg['sru-cycle-raw']
            sru_cycle_spin  = msg['sru-cycle-spin']
            kernel_source   = msg['kernel-source']
            kernel_version  = msg['kernel-version']
            kernel_flavour  = msg['kernel-flavour']
            series_codename = msg['series-codename']
            tests_running   = 0
            tests_passed    = 0
            tests_failed    = 0
            deploy_failed   = 0
            if msg['op'] == 'jobs.created':
                id = 'NULL'
                total_tests     = 0
                for arch in msg['arches']:
                    for instance_type in msg['arches'][arch]['instance-types']:
                        total_tests += len(msg['arches'][arch]['instance-types'][instance_type]['tests'])

        q  = 'insert or replace into test_stats ('
        q += 'id, '
        q += 'sru_cycle, '
        q += 'sru_cycle_raw, '
        q += 'sru_cycle_spin, '

        q += 'kernel_source, '
        q += 'kernel_version, '
        q += 'kernel_flavour, '

        q += 'series_codename, '

        q += 'total_tests, '
        q += 'tests_running, '
        q += 'tests_passed, '
        q += 'tests_failed, '
        q += 'deploy_failed) '
        q += 'values '
        q += '('
        q += f'{id}, '
        q += f'"{sru_cycle}", '
        q += f'"{sru_cycle_raw}", '
        q += f'"{sru_cycle_spin}", '
        q += f'"{kernel_source}", '
        q += f'"{kernel_version}", '
        q += f'"{kernel_flavour}", '
        q += f'"{series_codename}", '
        q += f'"{total_tests}", '
        q += f'"{tests_running}", '
        q += f'"{tests_passed}", '
        q += f'"{tests_failed}", '
        q += f'"{deploy_failed}" '
        q += ');'

        try:
            cursor = self.sql.cursor()
            cursor.execute(q)
            self.sql.commit()
            pre(q)
        except sqlite3.OperationalError:
            pre('Exception thrown executing:\n    %s\n' % q)
            pre(q)
            raise

class TJDB(SQLBase):
    def __init__(self):
        self.db_path = '/home/work/ksc/messages-to-db/jobs.sql3'
        super(TJDB, self).__init__(self.db_path)
        self.init_schema()

    def init_schema(self):
        self.init_schema_jobs_table()

    def init_schema_jobs_table(self):
        try:
            cursor = self.sql.cursor()

            q  = 'create table if not exists '
            q += 'jobs ( '
            q += '    id                         integer primary key,'    # A unique identifier
            q += '    cloud                      text,'                   #
            q += '    region                     text,'                   #
            q += '    instance_type              text,'                   #
            q += '    request_type               text,'                   # Why the jobs was created ('sru', 'boot', etc.)
            q += '    sru_cycle                  text,'                   # The SRU cycle id (without the spin #)
            q += '    sru_cycle_raw              text,'                   # SRU cycle id with spin #
            q += '    sru_cycle_spin             text,'                   # SRU cycle spin
            q += '    series_codename            text,'                   # Xenial, Bionic, Focal, etc.
            q += '    series_name                text,'                   # 16.04, 18.04, 20.04, etc.
            q += '    package_name               text,'                   # Kernel package name
            q += '    package_version            text,'                   # Package version
            q += '    package_flavour            text,'                   # Kernel flavour (generic, lowlatency, aws, azure, etc.)
            q += '    job_name                   text,'                   # The jenkins job name
            q += '    job_created_timestamp      text,'                   #
            q += '    start_delay                text,'                   #
            q += '    job_start_timestamp        text,'                   # Job started timestamp
            q += '    job_finish_timestamp       text,'                   # Job finished timestamp
            q += '    job_duration               text,'                   # Duration of job (finish timestamp - start timestamp)
            q += '    test_name                  text,'                   #
            q += '    results_passed             text,'                   # Number of tests that passed
            q += '    results_failed             text,'                   # Number of tests that failed
            q += '    results_summary            text,'                   # passed/failed
            q += '    prep_start_timestamp       text,'                   #
            q += '    prep_finish_timestamp      text,'                   #
            q += '    prep_duration              text,'                   #
            q += '    prep_status                text,'                   #
            q += '    deploy_start_timestamp     text,'                   #
            q += '    deploy_finish_timestamp    text,'                   #
            q += '    deploy_duration            text,'                   #
            q += '    deploy_status              text,'                   #
            q += '    test_start_timestamp       text,'                   #
            q += '    test_finish_timestamp      text,'                   #
            q += '    test_duration              text,'                   #
            q += '    test_status                text'                    #
            q += ');'

            cursor.execute(q)
            self.sql.commit()
        except Exception as e:
            self.sql.rollback()
            self.sql.close()
            raise e

    def find_job(self, info):
        q  = 'select * from jobs where '
        q += f'package_name = "{info["kernel-package"]}" '
        q += f'and package_version = "{info["kernel-version"]}" '
        q += f'and package_flavour = "{info["kernel-flavour"]}" '
        q += f'and cloud = "{info["cloud"]}" '
        q += f'and region = "{info["region"]}" '
        q += f'and series_codename = "{info["series-codename"]}" '
        q += f'and sru_cycle_raw = "{info["sru-cycle-raw"]}" '
        q += f'and job_name = "{info["test-job-name"]}"'
        q += ';'

        # pre(f' query: {q}')
        found = self.fetch_all(q)
        try:
            if len(found) > 1:
                pre(Color.style('  ** Exception BadIdQueryError: %d records returned' % len(found), Color.fg('red')))
                pre(json.dumps(info, sort_keys=True, indent=4))
                raise BadIdQueryError()
            elif len(found) < 1:
                # pre(CS('No matching record was found (len(found) < 1)', CF('yellow')))
                raise NoRecordsFoundError()
        except TypeError:
            # pre(CS('No matching record was found', CF('yellow')))
            raise NoRecordsFoundError()
        # pre(CS(f'found: {found[0]["id"]}', CF('yellow')))
        return found

    def duration(self, lh, rh):
        if lh == '""' or rh == '""':
            return '"-1"'
        return f'"{str(to_date(lh) - to_date(rh))}"'

    def update(self, msg, debug=False):
        defaults = OrderedDict()
        defaults['id'                        ] = 'NULL'
        defaults['cloud'                     ] = '""'
        defaults['region'                    ] = '""'
        defaults['instance_type'             ] = '""'
        defaults['request_type'              ] = '""'
        defaults['sru_cycle'                 ] = '""'
        defaults['sru_cycle_raw'             ] = '""'
        defaults['sru_cycle_spin'            ] = '""'
        defaults['series_codename'           ] = '""'
        defaults['series_name'               ] = '""'
        defaults['package_name'              ] = '""'
        defaults['package_version'           ] = '""'
        defaults['package_flavour'           ] = '""'
        defaults['job_name'                  ] = '""'
        defaults['job_created_timestamp'     ] = '""'
        defaults['start_delay'               ] = '""'
        defaults['job_start_timestamp'       ] = '""'
        defaults['job_finish_timestamp'      ] = '""'
        defaults['job_duration'              ] = '""'
        defaults['test_name'                 ] = '""'
        defaults['results_passed'            ] = '"0"'
        defaults['results_failed'            ] = '"0"'
        defaults['results_summary'           ] = '""'
        defaults['prep_start_timestamp'      ] = '""'
        defaults['prep_finish_timestamp'     ] = '""'
        defaults['prep_duration'             ] = '"00:00:00"'
        defaults['prep_status'               ] = '""'
        defaults['deploy_start_timestamp'    ] = '""'
        defaults['deploy_finish_timestamp'   ] = '""'
        defaults['deploy_duration'           ] = '"00:00:00"'
        defaults['deploy_status'             ] = '""'
        defaults['test_start_timestamp'      ] = '""'
        defaults['test_finish_timestamp'     ] = '""'
        defaults['test_duration'             ] = '"00:00:00"'
        defaults['test_status'               ] = '""'

        neo = OrderedDict()
        for k in defaults:
            neo[k] = defaults[k]

        try:
            recs = self.find_job(msg)
            rec = recs[0]
            for k in defaults:
                neo[k] = f'"{rec[k]}"'
            neo['id'] = rec['id']
        except NoRecordsFoundError:
            pass

        m = {
            'cloud'           : 'cloud',
            'region'          : 'region',
            'instance_type'   : 'instance-type',
            'request_type'    : 'original-op',
            'sru_cycle_raw'   : 'sru-cycle-raw',
            'sru_cycle'       : 'sru-cycle',
            'sru_cycle_spin'  : 'sru-cycle-spin',
            'series_codename' : 'series-codename',
            'series_name'     : 'series-name',
            'package_name'    : 'kernel-package',
            'package_version' : 'kernel-version',
            'package_flavour' : 'kernel-flavour',
            'job_name'        : 'test-job-name',
            'test_name'       : 'test',
        }
        for k in defaults:
            try:
                if m[k] in msg:
                    neo[k] = f'"{msg[m[k]]}"'
            except KeyError:
                pass

        while True:
            # Job
            #
            if msg['op'] == 'job.created':
                neo['job_created_timestamp']   = f'"{msg["timestamp"]}"'
                break

            if msg['op'] == 'job.finished':
                neo['job_finish_timestamp']    = f'"{msg["timestamp"]}"'
                neo['job_duration']        = self.duration(msg["timestamp"], neo["job_start_timestamp"])
                break

            if msg['op'] == 'job.started':
                neo['job_start_timestamp']     = f'"{msg["timestamp"]}"'
                neo['start_delay'] = self.duration(msg["timestamp"], neo['job_created_timestamp'])
                break

            # Deploy
            #
            if msg['op'] == 'sut.deploy.started':
                neo['deploy_start_timestamp']  = f'"{msg["timestamp"]}"'
                break

            if msg['op'] == 'sut.deploy.succeeded':
                neo['deploy_finish_timestamp']  = f'"{msg["timestamp"]}"'
                neo['deploy_duration']     = self.duration(msg["timestamp"], neo["deploy_start_timestamp"])
                neo['deploy_status'] = '"succeeded"'

                break

            if msg['op'] == 'sut.deploy.failed':
                neo['deploy_finish_timestamp'] = f'"{msg["timestamp"]}"'
                neo['deploy_duration']     = self.duration(msg["timestamp"], neo["deploy_start_timestamp"])
                neo['deploy_status'] = '"failed"'
                break

            # Prep
            #
            if msg['op'] == 'sut.prep.started':
                neo['prep_start_timestamp']    = f'"{msg["timestamp"]}"'
                break

            if msg['op'] == 'sut.prep.succeeded':
                neo['prep_finish_timestamp']   = f'"{msg["timestamp"]}"'
                neo['prep_duration']       = self.duration(msg["timestamp"], neo["prep_start_timestamp"])
                neo['prep_status'] = '"succeeded"'
                break

            if msg['op'] == 'sut.prep.failed':
                neo['prep_finish_timestamp']   = f'"{msg["timestamp"]}"'
                neo['prep_duration']       = self.duration(msg["timestamp"], neo["prep_start_timestamp"])
                neo['prep_status'] = '"failed"'
                break

            # Testing
            #
            if msg['op'] == 'sut.testing.started':
                neo['test_start_timestamp']    = f'"{msg["timestamp"]}"'
                break

            if msg['op'] == 'sut.testing.completed':
                neo['test_finish_timestamp']   = f'"{msg["timestamp"]}"'
                neo['test_duration']       = self.duration(msg["timestamp"], neo["test_start_timestamp"])
                neo['test_status'] = '"succeeded"'
                break

            if msg['op'] == 'sut.testing.failed':
                neo['test_finish_timestamp']   = f'"{msg["timestamp"]}"'
                neo['test_duration']       = self.duration(msg["timestamp"], neo["test_start_timestamp"])
                neo['test_status'] = '"failed"'
                break

            break

        q  = 'insert or replace into jobs ('
        q += ','.join(list(neo.keys()))
        q += ') values ('
        q += str(neo['id']) + ','
        q += ','.join(list(neo.values())[1:])
        q += ');'
        try:
            cursor = self.sql.cursor()
            cursor.execute(q)
            self.sql.commit()
            if debug:
                pre('Note: New record added')
        except sqlite3.OperationalError:
            if debug:
                pre(Color.style('    2', CYAN))

            pre('Exception thrown executing:\n    %s\n' % q)
            raise


class DBHelp(TJDB):
    def __init__(self):
        TJDB.__init__(self)

    # def sru_cycles(self):
    #     return self.fetch_all('select distinct sru_cycle from testing_jobs order by sru_cycle')

    # def packages_of_cycle_and_series(self, sru_cycle, series_codename):
    #     return self.fetch_all(f'select distinct package_name,package_version from testing_jobs where sru_cycle = "{sru_cycle}" and series_codename = "{series_codename}"')

    # def packages_of_cycle(self, sru_cycle):
    #     return self.fetch_all(f'select * from testing_jobs where sru_cycle = "{sru_cycle}" order by package_name')

    # def series_of_cycle(self, sru_cycle):
    #     return self.fetch_all(f'select distinct series_codename from testing_jobs where sru_cycle = "{sru_cycle}" order by series_codename')

    # def ui_of_cycle_and_series_package_and_version(self, sru_cycle, series_codename, package_name, package_version):
    #     q = f'select ui from testing_jobs where sru_cycle = "{"sru_cycle"}" and series_codename = "{series_codename}" and package_name = "{package_name}" and package_version = "{package_version}";'
    #     return self.fetch_all(q)

    # def list_from_query(self, query, field):
    #     retval = []
    #     recs = self.fetch_all(query)
    #     for rec in recs:
    #         retval.append(rec[field])
    #     return retval

# vi:set ts=4 sw=4 expandtab syntax=python:
