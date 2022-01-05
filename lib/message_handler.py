#!/usr/bin/env python3
#

import json
from db.testing_jobs_db                 import TestStatsDB, MessageError, MissingRecordError, TJDB
from lib.std                            import pre, prej, CF, CS
from lib.kernel_series                  import KernelSeries

INFO      = CF('green')
CYAN      = CF('cyan')
TIMESTAMP = CF('grey_50')
SERIES    = CF('cyan')
PACKAGE   = CF('dark_orange')
FLAVOUR   = CF('dark_orange')
VERSION   = CF('dark_orange')
CYCLE     = CF('yellow')
SPIN      = CF('green')
LOOK      = CF('magenta')


class MessageHandler():

    # __init__
    #
    def __init__(self, archive_messages=True):
        self.messages            = []
        self.message_count       = 0
        self.total_message_count = 0
        self.log_count           = 1
        self.__jobs_db           = None
        self.__stats_db          = None
        self.__archive_messages  = archive_messages
        self.series_codename_to_name = {}
        self.ks    = KernelSeries()

    @property
    def jobs_db(self):
        if self.__jobs_db is None:
            pre('Establishing connection to database ...')
            self.__jobs_db = TJDB()
            pre('Database connection established')
        return self.__jobs_db

    @property
    def stats_db(self):
        if self.__stats_db is None:
            pre('Establishing connection to database ...')
            self.__stats_db = TestStatsDB()
            pre('Database connection established')
        return self.__stats_db

    def __prmsg(self, msg):
        try:
            if 'sru-cycle-raw' in msg and '-' in msg['sru-cycle-raw']:
                (cycle, spin) = msg["sru-cycle-raw"].split('-', 1)
            else:
                (cycle, spin) = msg["sru-cycle"].split('-', 1)
            o  = ''
            o += CS(f'{msg["timestamp"]:40s}', TIMESTAMP)
            o += CS(f'{msg["op"]:25s}', CYAN)
            try:
                o += CS(f'{msg["test-job-name"]:100s}', CYAN)
            except KeyError:
                jn = 'unknown'
                o += CS(f'{jn:100s}', CYAN)

            o += CS(f'{cycle:<12}', CYCLE)
            o += CS(f'{spin:<4}', SPIN)
            try:
                o += CS(f'{msg["series-name"]:<12}', SERIES)
            except KeyError:
                o += CS(f'{msg["series-codename"]:<12}', SERIES)
            try:
                o += CS(f'{msg["kernel-package"]:24}', PACKAGE)
            except KeyError:
                o += CS(f'{msg["kernel-source"]:24}', PACKAGE)
            o += CS(f'{msg["kernel-flavour"]:16}', FLAVOUR)
            o += CS(f'{msg["kernel-version"]:24}', VERSION)
            o += CS(f'{msg["cloud"]:12}', CYAN)
            o += CS(f'{self.total_message_count} ', CYCLE)
            if self.__archive_messages:
                o += CS(f'{self.message_count} ', CYCLE)

            pre(o)
        except (KeyError, ValueError):
            if msg["sru-cycle"] == 'CRD.2021.06.21':
                return
            else:
                prej(msg)
                raise

    def __archive(self, msg):
        if self.__archive_messages:
            self.messages.append(msg)
            self.message_count += 1
            if self.message_count == 100:
                self.message_count = 0
                with open(f'messages.log.{self.log_count}', 'w') as fd:
                    fd.write(json.dumps(self.messages, sort_keys=True, indent=4))
                self.log_count += 1

    # flush
    #
    def flush(self, request):
        pre(CS('        ---------------------------------------   f l u s h   ---------------------------------------', INFO))
        prej(request)
        pre(CS('flushing: %s  %s  %s  %s' % (request['key'], request['package'], request['series-name'], request['kernel-version']), INFO))

    # handle
    #
    def handle(self, msg):
        # pre(CS('        ---------------------------------------  h a n d l e  ---------------------------------------', INFO))
        self.total_message_count += 1
        # self.__archive(msg)
        self.__prmsg(msg)

        try:
            series_codename = msg['series-name']
        except KeyError:
            series_codename = msg['series-codename']

        series_name = ''
        try:
            series_name = self.series_codename_to_name[series_codename.capitalize()]
        except KeyError:
            series_name = self.ks.lookup_series(codename=series_codename).name
            self.series_codename_to_name[series_codename.capitalize()] = series_name
        if 'sru-cycle-raw' not in msg:
            msg['series-name'] = series_name
            msg['series-codename'] = series_codename
            msg['sru-cycle-raw'] = msg['sru-cycle']
            (msg['sru-cycle'], msg['sru-cycle-spin']) = msg['sru-cycle-raw'].split('-', 1)

        try:
            if msg['op'] in ['job.created', 'job.started', 'job.finished']:
                self.jobs_db.update(msg)
            elif msg['op'] in ['jobs.created']:
                pre(CS('*** LOOK AT ME ***', LOOK))
                self.stats_db.update(msg)
                pass # Eventually do something real with these
            elif msg['op'] == 'sut.testing.results':
                self.jobs_db.update(msg)
            elif msg['op'] in ['sut.prep.started', 'sut.prep.succeeded', 'sut.prep.failed', 'sut.deploy.started', 'sut.deploy.succeeded', 'sut.deploy.failed', 'sut.testing.started', 'sut.testing.completed', 'sut.testing.failed']:
                self.jobs_db.update(msg)
            elif msg['op'] == 'ingest.complete':
                if msg['test-job-name'].endswith('--master--ingest'):
                    pre(CS(f'{msg["op"]:25s} master job', CF('magenta')))
                else:
                    pass # Not doing anything with these at this time
            else:
                pre(CS(f'{msg["op"]:25s} unhandled', CF('magenta')))
                # self.messages.append(msg)
                # with open('unhandled.log', 'w') as fid:
                #     fid.write(json.dumps(self.messages, sort_keys=True, indent=4))
                # pass # just drop it
        except MessageError:
            pass
        except MissingRecordError:
            msg['ERROR'] = 'MissingRecord'
