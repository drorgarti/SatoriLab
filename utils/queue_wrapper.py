import pymongo
from datetime import datetime, timedelta
import traceback


DEFAULT_INSERT = {
    "priority": 0,
    "attempts": 0,
    "locked_by": None,
    "locked_at": None,
    "last_error": None
}

class QueueWrapper:


    def __init__(self, db, name, consumer_id, timeout=300, max_attempts=3):
        """
        """
        self.collection = db[name]
        #self.collection = collection
        self.consumer_id = consumer_id
        self.timeout = timeout
        self.max_attempts = max_attempts

    def _unit_test_(self):
        try:
            # Push 3 elements
            self.put("dror", 1)
            self.put("dror", 2)
            self.put("dror", 3)
            # pop 2 elements
            val = self.next()
            val = self.next()
            val = self.next()
        except Exception as e:
            print("failed. ", e)

    def _jobs(self):
        return self.collection.find(
            query={"locked_by": None,
                   "locked_at": None,
                   "attempts": {"$lt": self.max_attempts}},
            sort=[('priority', pymongo.DESCENDING)],
        )

    def _wrap_one(self, data):
        return data and QueueJob(self, data) or None

    def close(self):
        """Close the in memory queue connection.
        """
        self.collection.connection.close()

    def clear(self):
        """Clear the queue.
        """
        return self.collection.drop()

    def size(self):
        """Total size of the queue
        """
        return self.collection.count()

    def repair(self):
        """Clear out stale locks.

        Increments per job attempt counter.
        """
        self.collection.find_and_modify(
            query={
                "locked_by": {"$ne": None},
                "locked_at": {
                    "$lt": datetime.now() - timedelta(self.timeout)}},
            update={
                "$set": {"locked_by": None, "locked_at": None},
                "$inc": {"attempts": 1}}
        )

    def put_multiple(self, payloads, priority=0):
        for payload in payloads:
            self.put(payload, priority)

    def put(self, payload, priority=0):
        """Place a job into the queue
        """
        job = dict(DEFAULT_INSERT)
        job['priority'] = priority
        job['payload'] = payload
        #return self.collection.update(key, data, {upsert: true});
        return self.collection.insert(job)

    def next(self):
        return self._wrap_one(self.collection.find_and_modify(
            query={"locked_by": None,
                   "locked_at": None,
                   "attempts": {"$lt": self.max_attempts}},
            update={"$set": {"attempts": 1,
                             "locked_by": self.consumer_id,
                             "locked_at": datetime.now()}},
            sort=[('priority', pymongo.DESCENDING)],
            new=1,
            limit=1
        ))


class QueueJob(object):

    def __init__(self, queue, data):
        """
        """
        self._queue = queue
        self._data = data

    @property
    def data(self):
        return self._data

    @property
    def payload(self):
        return self._data['payload']

    @property
    def job_id(self):
        return self._data["_id"]

    ## Job Control

    def complete(self):
        """Job has been completed.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            remove=True)

    def error(self, message=None):
        """Note an error processing a job, and return it to the queue.
        """
        self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {
                "locked_by": None, "locked_at": None, "last_error": message},
                "$inc": {"attempts": 1}})

    def progress(self, count=0):
        """Note progress on a long running task.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"progress": count, "locked_at": datetime.now()}})

    def release(self):
        """Put the job back into_queue.
        """
        return self._queue.collection.find_and_modify(
            {"_id": self.job_id, "locked_by": self._queue.consumer_id},
            update={"$set": {"locked_by": None, "locked_at": None},
                    "$inc": {"attempts": 1}})

    ## Context Manager support

    def __enter__(self):
        return self.data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)

