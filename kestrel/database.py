# Kestrel: An XMPP-based Many-Task Computing Scheduler
# Copyright (C) 2009-2010 Lance Stout
# This file is part of Kestrel.
#
# Kestrel is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Kestrel is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Kestrel. If not, see <http://www.gnu.org/licenses/>.

import logging
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey, and_, or_, select, func
from sqlalchemy.orm import mapper, relationship, sessionmaker, object_session, column_property


class RosterItem(object):
    def __init__(self):
        self.session = object_session(self)

    def set_show(self, state):
        show_map = {None: None,
                    'available': 'chat',
                    'busy': 'dnd',
                    'offline': None,
                    'queued': 'away',
                    'pending': 'away',
                    'running': 'chat',
                    'cancelling': 'dnd',
                    'cancelled': 'dnd',
                    'completed': 'xa'}
        self.show = show_map.get(state, None)

    def subscribe(self):
        self.subscription_to = 1

    def subscribed(self):
        self.subscription_from = 1

    def unsubscribe(self):
        self.subscription_to = 0

    def unsubscribed(self):
        self.subscription_from = 0


class Worker(object):
    def __init__(self):
        self.session = object_session(self)

    def provides(self, capabilities):
        if capabilities is not None:
            capabilities.sort()
            self.capabilities = '%' + ('%'.join(capabilities)).upper() + '%'

    def set_state(self, state):
        if state == self.state:
            return False
        self.state = state
        if self.roster is not None:
            self.roster.set_show(state)
        return True

    def available(self):
        self.set_state('available')

    def busy(self):
        self.set_state('busy')

    def offline(self):
        self.set_state('offline')
        jobs = set()
        for task in self.tasks:
            task.reset()
            jobs.add(task.job)
        return jobs


class Job(object):
    def __init__(self):
        self.session = object_session(self)
        
    def convert(**args):
        if args.get('jid', False):
            jid = args.get('jid')
            try:
                return jid[jid.index('job_')+4:jid.index('@')]
            except:
                return None
        if args.get('id', False):
            task = args.get('task', None)
            if task is None:
                task = ''
            else:
                task = '/' + str(task)
            args['task'] = task
            return 'job_%(id)s@%(base)s%(task)s' % args
        return None
    convert = staticmethod(convert)

    def queue_tasks(self, size):
        logging.info("JOB: Job %s queued with %s tasks for %s." % (self.id, 
                                                                   size,
                                                                   self.owner))
        self.queue = size
        for i in xrange(0, size):
            task = Task()
            task.task_id = i
            task.queued()
            self.tasks.append(task)

    def requires(self, requirements):
        if requirements is None:
            requirements = '%'
        else:
            requirements.sort()
            requirements = '%' + '%'.join(requirements) + '%'
        self.requirements = requirements.upper()

    def set_status(self, status):
        self.status = status
        self.roster.set_show(status)
        
    def wait(self, force=False):
        self.set_status('queued')
        
    def run(self):
        self.set_status('running')

    def complete(self, force=False):
        logging.info("JOB: Job %s completed." % self.id)
        self.set_status('completed')

    def cancel(self):
        logging.info("JOB: Job %s cancelled by %s." % (self.id, self.owner))
        self.set_status('cancelled')
        cancelled_tasks = []
        for task in self.tasks:
            if task.cancel():
                cancelled_tasks.append(task)
        return cancelled_tasks

    def summary(self, condensed=False):
        result = {'owner': self.owner,
                  'requested': self.queue,
                  'queued': self.num_queued,
                  'running': self.num_running,
                  'completed': self.num_completed} 
        if condensed:
            return "(%(requested)s) %(queued)s/%(running)s/%(completed)s" % result
        return result


class Task(object):
    def __init__(self):
        self.session = object_session(self)

    def queued(self):
        self.status = 'queued'

    def pending(self, worker):
        logging.info("TASK: Task (%s, %s) assigned to %s." % (self.job_id, 
                                                              self.task_id,
                                                              worker.jid))
        self.status = 'pending'
        self.worker = worker

    def cancel(self):
        logging.info("TASK: Task (%s, %s) cancelled." % (self.job_id, 
                                                         self.task_id))
        transitions = {'queued': 'completed',
                       'pending': 'completed',
                       'running': 'cancelling',
                       'cancelling': 'cancelling',
                       'completed': 'completed'}
        self.status = transitions.get(self.status, 'completed')

    def reset(self):
        logging.info("TASK: Task (%s, %s) reset." % (self.job_id, 
                                                     self.task_id))
        transitions = {'queued': 'queued',
                       'pending': 'queued',
                       'running': 'queued',
                       'cancelling': 'completed',
                       'completed': 'completed'}
        self.worker = None
        self.status = transitions.get(self.status, 'queued')

    def finish(self):
        logging.info("TASK: Task (%s, %s) completed by %s." % (self.job_id, 
                                                               self.task_id,
                                                               self.worker.jid))
        self.status = 'completed'
        self.worker_id = None

    def start(self):
        logging.info("TASK: Task (%s, %s) started by %s." % (self.job_id, 
                                                             self.task_id,
                                                             self.worker.jid))
        self.status = 'running'
        self.job.run()


class Database(object):
    """Create and manage a database connection"""

    def __init__(self, source):
        self.engine = sql.create_engine(source+'?check_same_thread=False')
        self.metadata = sql.MetaData()

        # ==============================================================
        # Database table definitions:
        # ==============================================================

        # --------------------------------------------------------------
        # Roster
        # --------------------------------------------------------------
        self.roster = Table('roster', self.metadata,
                            Column('owner', String, primary_key=True),
                            Column('jid', String, primary_key=True),
                            Column('subscription_to', Integer),
                            Column('subscription_from', Integer),
                            Column('show', String))

        # --------------------------------------------------------------
        # Tasks
        # --------------------------------------------------------------
        self.tasks = Table('tasks', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('job_id', Integer, ForeignKey('jobs.id')),
                           Column('task_id', Integer),
                           Column('worker_id', Integer, ForeignKey('workers.jid')),
                           Column('status', String))

        # --------------------------------------------------------------
        # Jobs
        # --------------------------------------------------------------
        self.jobs = Table('jobs', self.metadata,
                          Column('id', Integer, primary_key=True),
                          Column('owner', String),
                          Column('jid', String, ForeignKey('roster.owner')),
                          Column('command', String),
                          Column('cleanup', String),
                          Column('queue', Integer),
                          Column('status', String),
                          Column('requirements', String))

        # --------------------------------------------------------------
        # Workers
        # --------------------------------------------------------------
        self.workers = Table('workers', self.metadata,
                             Column('jid', String, ForeignKey('roster.owner'), 
                                    primary_key=True),
                             Column('state', String),
                             Column('capabilities', String))

        # --------------------------------------------------------------
        # Object Relational Mappers
        # --------------------------------------------------------------
        mapper(RosterItem, self.roster)

        mapper(Job, self.jobs, properties={
                'tasks': relationship(Task, backref='job'),
                'roster': relationship(RosterItem, backref='job'),
                'num_queued': column_property(
                    select([func.count(self.tasks.c.id)],
                           and_(self.tasks.c.job_id==self.jobs.c.id,
                                or_(self.tasks.c.status=='queued',
                                    self.tasks.c.status=='pending'))).label('num_queued')),
                'num_running': column_property(
                    select([func.count(self.tasks.c.id)],
                           and_(self.tasks.c.job_id==self.jobs.c.id,
                                self.tasks.c.status=='running')).label('num_running')),
                'num_completed': column_property(
                    select([func.count(self.tasks.c.id)],
                           and_(self.tasks.c.job_id==self.jobs.c.id,
                                or_(self.tasks.c.status=='cancelling',
                                    self.tasks.c.status=='completed'))).label('num_completed'))
                })

        mapper(Worker, self.workers, properties={
                'tasks': relationship(Task, backref='worker'),
                'roster': relationship(RosterItem, backref='worker')
                })

        mapper(Task, self.tasks)
        # --------------------------------------------------------------

        self.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def session(self):
        """Create a new database session."""
        return self.Session()
