from .common import *

import json
import hashlib

class Job:
    def __init__(self, _id=None, _job_metadata=None, _force=False, **kwargs):
        from .context import c as context

        self.c = context
        self.db = self.c.db

        if _job_metadata is None:
            _job_metadata = {}

        name = self.__class__.__name__

        if _id is None:

            # very useful for retrieving the job later
            kwargs = self.c.compress_args(kwargs)
            kwargs_hash = hashlib.sha256(json.dumps({'name': name, 'args': kwargs}, sort_keys=True).encode()).hexdigest()

            # here we check if the job is already in the database
            # if it is, we load it
            if not _force:

                _find = self.db.find_one({
                    'hash': kwargs_hash,
                })

                if _find is not None:
                    self._info = _find
                    self.id = _find['_id']
                    return

            _def = {
                'type': name,
                'when': dt.now(),
                'args': kwargs,
                'hash': kwargs_hash,
                'claimed': None,
                'completed': False,
                'ready': False,
                **_job_metadata
            }
            
            self._info = _def
            self.save()

            d = self.extract_dependencies(self['args'])
            if len(d):
                self.push('trigger_ids', [x.id for x in d])

            for x in d:
                x.push('then', self.id)

        else:
            if type(_id) == str or type(_id) == ObjectId:

                self.id = ObjectId(_id)
                self._info = self.db.find_one({'_id': self.id})

            elif type(_id) == dict:

                self._info = _id

            else:
                raise ValueError("Argument must be an id, or a dictionary")

    # when an attribute doesn't exist, we try to pull it from self.info['args']
    def __getattr__(self, key):
        if key in self._info['args']:
            return self._info['args'][key]

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def extract_dependencies(self, a):
        """
        For any variable, it returns a list of dependencies.
        """

        a = self.c.expand_args(a)

        if isinstance(a, Job):
            return [self.c.load(a.id)]

        elif isinstance(a, dict):
            return [
                dep
                for v in a.values()
                for dep in self.extract_dependencies(v)
            ]

        elif isinstance(a, list):
            return [
                dep
                for x in a
                for dep in self.extract_dependencies(x)
            ]

        return []

    def queue(self):
        """
        This function propagates out to the leaves, starting all dependencies of dependences of...
        """
        # if there are dependencies, offload the responsibility to them
        deps = self.extract_dependencies(self['args'])
        if len(deps):
            for dep in self.extract_dependencies(self['args']):
                dep.queue()

        if not self['ready']:
            logger.info(f"Queueing task {self.__class__.__name__}")
            self['ready'] = True

        return self

    def then(self, other):
        """
        We store the 'other' as a next step, which can only be completed once this one is.
        We need an entry in the database entry for other:
            {'ready': {str(self._id): False}}
        And in the database entry for self:
            {'then': [str(other._id)]}

        When B is completed, it will set its own 'ready' value in the entry for A to True.
        """

        logger.info(f"Defining dependency {self.__class__.__name__} → {other.__class__.__name__}")

        # update this task to include the other as a next step
        self.push('then', other.id)

        # update the other task to include this task as a dependency
        other.push('trigger_ids', self.id)

        return other

    def run(self):
        raise NotImplementedError
    
    def run_wrapper(self):
        """
        This function is called by the worker, and is responsible for gathering the necessary arguments and running the task.
        """

        # gather the necessary arguments
        args = self.c.expand_args(self['args'])

        # run the task
        try:
            _result = self.run(**args)

        except Exception as e:
            # get the full traceback
            import traceback
            full_traceback = traceback.format_exc()
            logger.error(f"Task {self.__class__.__name__} failed with exception: {str(e)}\n{full_traceback}")

            # add the exception to the database
            self['exception'] = str(e)

            # and simply return None
            return None

        logger.info(f"Task {self.__class__.__name__} completed successfully.")

        # now that it's done successfully, 
        # 1. add the result to the database
        self['result'] = self.c.compress_args(_result)
        self['completed'] = True
            
        # 2. let the other tasks know
        for then_id in self.get('then', []):
            myid = self.id
            then_id = ObjectId(then_id)
            
            _upd = self.c.db.update_one(
                {'_id': then_id},
                {'$pull': {'trigger_ids': myid}}
            )

            logger.info(f"... triggering task {then_id} ({_upd.modified_count} records updated)")

        return _result

    def __call__(self, now=True):
        
        if now:
            if self['completed']:
                return self['result']
            
            return self.run_wrapper()
        
        else:
            # obviously we want it to be done, if we're calling it...
            if not self['completed']:
                self.queue()

            return {
                'done': self['completed'],
                'result': self['result'] if self['completed'] else None
            }

    # some helper functions
    def __contains__(self, key):
        return key in self._info
        
    def __getitem__(self, key):
        return self._info[key]
    
    def get(self, key, *args, **kwargs):
        if key in self._info:
            return self._info[key]
        
        if 'default' in kwargs:
            return kwargs['default']
        
        if len(args):
            return args[0]
        
        raise ValueError(f'Key not found in <{self.__class__}>')


    
    def __setitem__(self, key, value):
        self._info[key] = value

        # update it in MongoDB if it's already saved
        if hasattr(self, 'id'):
            self.db.update_one(
                {'_id': self.id},
                {'$set': {key: value}}
            )

    def save(self):
        if not hasattr(self, 'id'):
            self.id = self.db.insert_one(self._info).inserted_id
        else:
            self.db.update_one(
                {'_id': self.id},
                {'$set': self._info}
            )

        return self

    def push(self, k, v):
        if k not in self._info or type(self._info[k]) != list:
            self._info[k] = []

        if type(v) != list:
            v = [v]

        self._info[k] += v

        self.db.update_one(
            {'_id': self.id},
            {'$push': {k: {'$each': v}}}
        )

        return self