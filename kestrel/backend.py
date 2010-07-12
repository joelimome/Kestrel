import logging
import Queue as queue
import threading
from database import *

class Backend(object):

    def __init__(self, db, xmpp):
        self.xmpp = xmpp
        self.db = db.session()
        self.thread = threading.Thread(name='db_queue',
                                       target=self.start_thread)
        self.queue = queue.Queue()

        self.roster = RosterBackend(self)
        self.workers = WorkerBackend(self)
        self.jobs = JobBackend(self)
        self.tasks = TaskBackend(self)

        self.thread.daemon = True
        self.thread.start()

    def start_thread(self):
        while True:
            reply, pointer, args = self.queue.get(block=True)
            result = pointer(*args)
            if reply is not None:
                reply.put(result)

    def query(self, pointer, args=None):
        if args is None:
            args = tuple()
        out = queue.Queue()
        self.queue.put((out, pointer, args))
        return out.get(block=True)

    def update(self, obj):
        self.db.merge(obj)
        self.db.commit()

    def save(self, obj):
        self.db.add(obj)
        self.db.commit()

# ######################################################################

class BaseBackend(object):

    def __init__(self, backend):
        self.backend = backend
        self.db = self.backend.db
        self.query = self.backend.query
        self.update = self.backend.update
        self.save = self.backend.save
        self.xmpp = self.backend.xmpp
        self.event = self.xmpp.event

# ######################################################################

class RosterBackend(BaseBackend):

    # ------------------------------------------------------------------

    def get(self, owner):
        return self.query(self._get, (owner,))

    def _get(self, owner):
        items = self.db.query(RosterItem.jid).filter_by(owner=owner).all()
        self.db.commit()
        return [r[0] for r in items]

    # ------------------------------------------------------------------

    def states(self, owner):
        return self.query(self._states, (owner,))

    def _states(self, owner):
        items = self.db.query(RosterItem.jid, RosterItem.show).filter_by(owner=owner).all()
        self.db.commit()
        return [(r[0], r[1]) for r in items]

    # ------------------------------------------------------------------

    def state(self, owner):
        return self.query(self._state, (owner,))

    def _state(self, owner):
        item = self.db.query(RosterItem.show).filter_by(owner=owner).first()
        if item is not None:
            self.db.commit()
            if item:
                return item[0]
        return None

    # ------------------------------------------------------------------

    def set_state(self, owner, state):
        logging.warning("DEPRECATED: roster.set_state")
        return self.query(self._set_state, (owner, state))

    def _set_state(self, owner, state):
        items = self.db.query(RosterItem).filter_by(owner=owner).all()
        for item in items:
            item.show = state
            self.db.merge(item)
        self.db.commit()

    # ------------------------------------------------------------------

    def jids(self):
        return self.query(self._jids, tuple())

    def _jids(self):
        items = self.db.query(RosterItem.owner).all()
        self.db.commit()
        return [r[0] for r in items]

    # ------------------------------------------------------------------

    def sub_to(self, owner, jid):
        return self.query(self._sub_to, (owner, jid))

    def _sub_to(self, owner, jid):
        result = self.db.query(RosterItem).filter_by(owner=owner, jid=jid, subscription_to=1).count() > 0
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def sub_from(self, owner, jid):
        return self.query(self._sub_from, (owner, jid))

    def _sub_from(self, owner, jid):
        result = self.db.query(RosterItem).filter_by(owner=owner, jid=jid, subscription_from=1).count() > 0
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def has_sub(self, owner, jid):
        return self.query(self._has_sub, (owner, jid))

    def _has_sub(self, owner, jid):
        result = self.db.query(RosterItem).filter_by(owner=owner, jid=jid).count() > 0
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def clean(self):
        logging.info("Cleaning roster table.")
        return self.query(self._clean, tuple())

    def _clean(self):
        old = self.db.query(RosterItem).filter_by(subscription_to=0, subscription_from=0).all()
        for item in old:
            self.db.delete(item)
        self.db.commit()

    # ------------------------------------------------------------------

    def subscribe(self, owner, jid):
        return self.query(self._subscribe, (owner, jid))

    def _subscribe(self, owner, jid):
        entry = RosterItem()
        entry.owner = owner
        entry.jid = jid
        entry.subscribe()
        self.update(entry)

    # ------------------------------------------------------------------

    def subscribed(self, owner, jid):
        return self.query(self._subscribed, (owner, jid))

    def _subscribed(self, owner, jid):
        entry = RosterItem()
        entry.owner = owner
        entry.jid = jid
        entry.subscribed()
        self.update(entry)

    # ------------------------------------------------------------------

    def unsubscribe(self, owner, jid):
        return self.query(self._unsubscribe, (owner, jid))

    def _unsubscribe(self, owner, jid):
        entry = RosterItem()
        entry.owner = owner
        entry.jid = jid
        entry.unsubscribe()
        self.update(entry)

    # ------------------------------------------------------------------

    def unsubscribed(self, owner, jid):
        return self.query(self._unsubscribed, (owner, jid))

    def _unsubscribed(self, owner, jid):
        entry = RosterItem()
        entry.owner = owner
        entry.jid = jid
        entry.unsubscribed()
        self.update(entry)

# ######################################################################

class WorkerBackend(BaseBackend):

    # ------------------------------------------------------------------

    def status(self):
        return self.query(self._status, tuple())

    def _status(self):
        available = self.db.query(Worker).filter_by(state='available').count()
        busy = self.db.query(Worker).filter_by(state='busy').count()
        return {'online': str(available + busy),
                'available': str(available),
                'busy': str(busy)}

    # ------------------------------------------------------------------

    def add(self, jid, capabilities):
        return self.query(self._add, (jid, capabilities))

    def _add(self, jid, capabilities):
        worker = Worker()
        worker.jid = jid
        worker.provides(capabilities)
        self.update(worker)
        worker.set_state('offline')
        self.update(worker)

    # ------------------------------------------------------------------

    def set_state(self, jid, state):
        return self.query(self._set_state, (jid, state))

    def _set_state(self, jid, state):
        worker = self.db.query(Worker).filter_by(jid=jid).one()
        result = worker.set_state(state)
        self.update(worker)
        return result

    # ------------------------------------------------------------------

    def known(self, jid):
        return self.query(self._known, (jid,))

    def _known(self, jid):
        result = self.db.query(Worker).filter_by(jid=jid).count() == 1
        self.db.commit()
        return result

    # ------------------------------------------------------------------

    def clean(self):
        return self.query(self._clean, tuple())

    def _clean(self):
        logging.info('Cleaning worker table.')
        self.db.query(Worker).delete()
        self.db.commit()

    # ------------------------------------------------------------------

    def match(self, worker_jid):
        return self.query(self._match, (worker_jid,))

    def _match(self, worker_jid):
        worker = self.db.query(Worker).filter_by(jid=worker_jid).one()
        caps = '%'+worker.capabilities.replace(' ', '%') + '%'

        where = and_(or_(Job.status=='queued',
                         Job.status=='running'),
                     Worker.capabilities.like(Job.requirements),
                     Worker.jid==worker_jid,
                     Worker.state=='available')
        jobs = self.db.query(Job).join((Worker, Worker.jid==worker_jid)).filter(where).all()
        for job in jobs:
            task = self.db.query(Task).filter_by(job_id=job.id, status='queued').first()
            if task is None:
                continue
            task.pending(worker)
            self.db.merge(task)
            self.db.commit()
            return task
        return False

    # ------------------------------------------------------------------

    def reset(self, worker_jid):
        return self.query(self._reset, (worker_jid,))

    def _reset(self, worker_jid):
        worker = self.db.query(Worker).filter_by(jid=worker_jid).one()
        jobs = worker.offline()
        self.update(worker)
        for job in jobs:
            self.event('kestrel_broadcast_presence', job.jid)
        return [job.id for job in jobs]


# ######################################################################

class JobBackend(BaseBackend):

    def active_jobs(self):
        return self.db.query(Job).filter(and_(Job.status!='completed',
                                              Job.status!='cancelled')).all()

    def finished_jobs(self):
        return self.db.query(Job).filter(and_(Job.status=='completed',
                                              Job.status=='cancelled')).all()

    def running_jobs(self):
        return self.db.query(Job).filter_by(status='running').all()

    def queued_jobs(self):
        return self.db.query(Job).filter_by(status='queued').all()

    def job(self, job_id, owner=None):
        where = or_(Job.id==job_id, Job.jid==job_id)
        if owner is not None:
            where = and_(where, Job.owner==owner)
        
        try:
            return self.db.query(Job).filter(where).one()
        except:
            logging.warning("Job %s not found." % job_id)
            return None

    # ------------------------------------------------------------------

    def status(self, job_id=None):
        return self.query(self._status, (job_id,))

    def _status(self, job_id=None):
        if job_id is None:
            statuses = {}
            for job in self.active_jobs():
                statuses[job.id] = job.summary()
            return statuses
        else:
            job = self.job(job_id)
            if job is None:
                return False
            return job.summary()

    # ------------------------------------------------------------------

    def queue(self, owner, jid=None, command='', cleanup=None, queue=1, requires=None):
        return self.query(self._queue, (owner, command, cleanup, queue, requires))

    def _queue(self, owner, command, cleanup=None, queue=1, requires=None):
        job = Job()
        job.owner = owner
        job.command = command
        job.cleanup = cleanup
        job.requires(requires)
        self.save(job)
        job.queue_tasks(queue)
        job.jid = Job.convert(id=job.id, base=self.xmpp.fulljid)
        self.update(job)
        self.event('kestrel_broadcast_presence', job.jid)
        return job.id

    # ------------------------------------------------------------------

    def create_jid(self, job_id, base_jid, task_id=''):
        return Job.convert(id=job_id, base=base_jid, task=task_id)
        
    # ------------------------------------------------------------------

    def get_id(self, job_jid):
        return Job.convert(jid=job_jid)

    # ------------------------------------------------------------------

    def set_jid(self, job_id, job_jid):
        logging.warning("DEPRECATED: jobs.set_jid")
        return self.query(self._set_jid, (job_id, job_jid))

    def _set_jid(self, job_id, job_jid):
        job = self.job(job_id)
        if job is not None:
            job.jid = job_jid
            self.update(job)

    # ------------------------------------------------------------------

    def cancel(self, owner, job_id):
        return self.query(self._cancel, (owner, job_id))

    def _cancel(self, owner, job_id):
        job = self.job(job_id, owner=owner)
        if job is None:
            return []
        tasks = job.cancel()
        self.event('kestrel_broadcast_presence', job.jid)
        self.update(job)
        return tasks

    # ------------------------------------------------------------------

    def match(self, job_id):
        return self.query(self._match, (job_id,))

    def _match(self, job_id):
        job = self.db.query(Job).filter_by(id=job_id).one()
        result = self.db.query(Worker).filter(and_(Worker.state=='available',
                                                   Worker.capabilities.like(job.requirements)))
        self.db.commit()
        tasks = []
        for worker in result:
            task = self.db.query(Task).filter_by(job_id=job_id, status='queued').first()
            if task:
                task.pending(worker)
                self.update(task)
                tasks.append(task)
        return tasks
        
# ######################################################################

class TaskBackend(BaseBackend):

    def task(self, job_id, task_id):
        try:
            return self.db.query(Task).filter_by(job_id=job_id, 
                                                 task_id=task_id).one()
        except:
            logging.warning("Task (%s, %s) not found." % (job_id, task_id))
            return None

    def active_tasks(self):
        return self.db.query(Task).filter(or_(Task.status=='running',
                                              Task.status=='pending',
                                              Task.status=='cancelling')).all()

    # ------------------------------------------------------------------

    def clean(self):
        return self.query(self._clean, tuple())

    def _clean(self):
        logging.info('Cleaning tasks table')
        tasks = self.db.query(Task).all()
        for task in self.active_tasks():
            task.reset()
            self.update(task)

    # ------------------------------------------------------------------

    def finish(self, job_id, task_id):
        return self.query(self._finish, (job_id, task_id))

    def _finish(self, job_id, task_id):
        finished = False
        task = self.task(job_id, task_id)
        if task is None:
            return False

        task.finish()
        self.update(task)
        job = self.backend.jobs.job(job_id)
        if job is None:
            return False

        if job.num_queued + job.num_running == 0:
            job.complete()
            finished = True
        elif job.num_running > 0:
            job.run()
        else:
            job.wait()

        self.update(job)
        self.event('kestrel_broadcast_presence', job.jid)
        return finished

    # ------------------------------------------------------------------

    def start(self, job_id, task_id):
        return self.query(self._start, (job_id, task_id))

    def _start(self, job_id, task_id):
        task = self.task(job_id, task_id)
        if task is not None:
            task.start()
            self.update(task.job)
            self.event('kestrel_broadcast_presence', task.job.jid)

    # ------------------------------------------------------------------

    def reset(self, job_id, task_id):
        return self.query(self._start, (job_id, task_id))

    def _reset(self, job_id, task_id):
        task = self.task(job_id, task_id)
        if task is not None:
            task.reset()
            self.update(task)
            self.event('kestrel_broadcast_presence', task.job.jid)
