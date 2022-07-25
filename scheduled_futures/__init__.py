import contextvars
from dataclasses import dataclass, field
import itertools
import sys
from types import FunctionType, LambdaType
import types
from typing import Any, Callable, Optional, Tuple, Dict, List, Union
import functools
import logging
import threading
import time
import heapq
from queue import Empty
from concurrent.futures._base import PENDING, CANCELLED, CANCELLED_AND_NOTIFIED, Executor
from concurrent.futures import Future, wait, CancelledError
from concurrent.futures.thread import BrokenThreadPool, ThreadPoolExecutor, \
		_global_shutdown_lock, _shutdown, _threads_queues, _worker
import uuid
import weakref


__all__ = [
	'ScheduledFuture',
	'ScheduledThreadPoolExecutor',
]


__version__ = '1.2.0'


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


@functools.total_ordering
class ScheduledFuture(Future):
	"""A future scheduled by a `ScheduledThreadPoolExecutor`."""

	def __init__(self, pool: 'ScheduledThreadPoolExecutor', fn: Callable, 
		args: Tuple[Any], kwargs: Dict[str, Any], weak: Optional[bool] = None, 
		context: Optional[contextvars.Context] = None,
		period: float = 0, start: Optional[float] = None, name: Optional[str] = None) -> None:
		"""Init
		
		Args:
			pool: A `ScheduledThreadPoolExecutor` instance
			fn: The callable or a weakref to a callable to execute.
			args: Tuple of arguments to `fn`
			kwargs: Mapping of key-value arguments to `fn`
			weak: Optional. If True, a weakref to the `fn` will be created and stored
				in the future instead of `fn` itself. If False, `fn` will be stored as-is.
				If None and `fn` is a function, lambda functino, or weakref,
				`fn` will be stored as-is; otherwise, a weakref to it will be stored.
				If `fn` is a weakref, it will be stored as-is, regardless of this argument.
				Defaults to None.
			context: Optional. The `contextvars.Context` in which to run `fn`.
				Defaults to None, which runs in the current context.
			period: Optional. The number of seconds to delay between runs of this
				future. If 0, the future will run only once and can be treated like
				a regular `Future`.
			start: Optional. Epoch time when the future should first start executing.
				Defaults to None, which starts immediately.
			name: Optional. A name identifying the future. It does not need to be unique.
		"""
		super().__init__()
		self._weak = weak
		if self._weak is None:
			if isinstance(fn, FunctionType) or isinstance(fn, LambdaType) or \
				isinstance(fn, weakref.ref):
				self._weak = False
			else:
				self._weak = True
		if self._weak and not isinstance(fn, weakref.ref):
			if isinstance(fn, types.MethodType):
				fn = weakref.WeakMethod(fn)
			else:
				fn = weakref.ref(fn)
		self._fn = fn
		self._args = args or ()
		self._kwargs = kwargs or {}
		self._pool = pool
		self._runtime: float = 0
		self._executions: int = 0
		self._exceptions: int = 0
		self._start = start or time.time()
		self._period = period
		self._next_run_time = self._start
		self._name = name
		self._pending_cancel: bool = False
		if context is None:
			self._context = contextvars.copy_context()
		else:
			self._context = context
		self._id = uuid.uuid4().hex

	def __hash__(self) -> int:
		return hash(self._id)

	def __eq__(self, other: Any) -> bool:
		return self is other

	def __lt__(self, other: Any):
		if other is None:  # None is the sentinel value used by executor
			return True
		return self.next_run_time() < other.next_run_time()

	@property
	def name(self) -> Optional[str]:
		"""A user-assigned name given to the future."""
		return self._name

	@property
	def id(self) -> str:
		"""A uuid for the future, unique to every instance."""
		return self._id

	def runtime(self) -> float:
		"""Total number of seconds elapsed running this future."""
		with self._condition:
			return self._runtime

	def executions(self) -> int:
		"""Total number of executions for the future."""
		with self._condition:
			return self._executions

	def exceptions(self) -> int:
		"""Total number of exceptions which have occurred when running the future.
		
		CancelledError is not counted.
		"""
		with self._condition:
			return self._exceptions

	def average_runtime(self) -> Optional[float]:
		"""Return the number of seconds this future takes to execute on average.
		If the future has never run, None is returned."""
		with self._condition:
			if self._executions <= 0:
				return None
			return self._runtime / self._executions

	def next_run_time(self) -> float:
		"""Return the time, in seconds since epoch, when the future is scheduled
		to next run.
		
		If this is a periodic future, this value is recalculated at the end
		of each run before putting it back in the schedule.
		"""
		with self._condition:
			return self._next_run_time

	def start(self) -> float:
		"""Return the time when the future is first scheduled to start,
		in seconds since epoch."""
		with self._condition:
			return self._start

	def period(self) -> float:
		"""Return the number of seconds of delay between runs of 
		this future. If 0, the future is a one-off."""
		with self._condition:
			return self._period

	def set_period(self, val: float) -> None:
		"""Set the period, which will change the delay until
		the next execution the next time the future runs and is scheduled again."""
		with self._condition:
			self._period = val

	def is_periodic(self) -> bool:
		"""Return True if this is a periodic scheduled future; False otherwise."""
		with self._condition:
			return self._period > 0
	
	def pending_cancellation(self) -> bool:
		"""Return True if this is a periodic scheduled future and 
		a cancellation is pending; False otherwise."""
		with self._condition:
			return self._pending_cancel

	def cancel(self) -> bool:
		"""Attempt to cancel future, returning True if immediately 
		successful and False otherwise.
		
		Cancellation of a future may not be possible if it has already started,
		so this method will return False in that case.
		
		For periodic scheduled futures which could not be immediately cancelled,
		a pending cancellation flag is set which will be evaluated after it finishes
		its current run.

		If the future was immediately cancelled, the reference to it in the schedule
		will be removed so garbage collection of the referenced `fn`, `args`, and `kwargs`
		is not held up.
		"""
		success = super().cancel()
		with self._condition:
			# if not already cancelled and periodic, then we have to wait
			# for the future to pick up on the cancellation when it finishes
			# running
			if not success and self.is_periodic():
				self._pending_cancel = True
			# it is possible the future has left the schedule queue but also
			# not run yet, so it may have been cancelled but we could not remove
			# it from the schedule in which case the thread that picked it up will
			# handle the final steps of the cancellation process which also prevents
			# rescheduling if periodic
			if success and self._pool._delete(self):
				# if able to cancel the future and remove it from the schedule queue
				# before it is processed, we need to manually notify the waiters
				# since the executor will not be doing it
				# waiting for the executor to process the cancellation would require
				# waiting until the next run time for the future, which might be a long
				# way off, so immediate deletion is done instead  
				self.set_running_or_notify_cancel()
		return success

	def run(self):
		"""Run the future.
		
		Should only be called by an executor.
		"""
		if not self.set_running_or_notify_cancel():
			return

		if isinstance(self._fn, weakref.ref):
			func = self._fn()
			if func is None:
				if self.is_periodic():
					msg = 'Periodic future lost reference to callable and will ' + \
						'not be scheduled again.'
				else:
					msg = 'Future lost reference to callable and will not run.'
				self.set_exception(ReferenceError(msg))
				return  # once handle lost, obviously we cannot reschedule
		else:
			func = self._fn

		exc = None
		try:
			started = time.time()
			run_lag = (started - self.next_run_time())
			if run_lag > self._pool.late_run_limit:
				log.warning('Late to run scheduled future.',
					extra=dict(metadata=dict(run_lag=run_lag, late_run_limit=self._pool.late_run_limit,
						future_name=self._name, future_id=self._id)))
			result = self._context.run(func, *self._args, **self._kwargs)
		except CancelledError:
			with self._condition:
				self._pending_cancel = True
		except BaseException as e:
			with self._condition:
				self._exceptions += 1
			exc = e
			if self.is_periodic():
				log.exception('Periodic future raised exception.',
					extra=dict(metadata=dict(future_name=self._name, future_id=self._id)))
		
		with self._condition:
			self._executions += 1
			runtime = (time.time() - started)
			self._runtime += runtime
			if self.is_periodic() and runtime > self.period():
				log.warning('Periodic scheduled future runtime exceeded period.',
					extra=dict(metadata=dict(runtime=runtime, period=self.period(), 
						future_name=self._name, future_id=self._id)))
		
		if self.pending_cancellation():
			process = False
			with self._condition:
				if self._state in [CANCELLED, CANCELLED_AND_NOTIFIED]:
					process = False
				else:
					self._state = CANCELLED
					self._condition.notify_all()
					process = True
			if process:
				self._invoke_callbacks()
				self.set_running_or_notify_cancel()
		elif self.is_periodic():
			with self._condition:
				self._state = PENDING
				self._next_run_time = time.time() + self._period
				try:
					self._pool._schedule(self)
				except RuntimeError:
					# pool shut down
					pass
		elif exc:
			self.set_exception(exc)
		else:
			self.set_result(result)


@dataclass(order=True)
class _WorkItem:
	priority: Optional[float]
	counter: int
	future: ScheduledFuture = field(compare=False)
	deleted: bool = field(default=False, compare=False)


class _ScheduledFutureQueue:

	def __init__(self):
		self._available = threading.Condition()
		self._counter = itertools.count()  # for fifo tie-breaking
		self._queue: List[_WorkItem] = []
		self._index: Dict[str, _WorkItem] = {}  # for deletion handling

	def put(self, future: Optional[ScheduledFuture]):
		with self._available:
			empty = not(bool(self._queue))
			nextwi = None
			if not empty:
				nextwi = self._queue[0]
			# None is a sentinel used by the executor to kill workers and needs
			# to always show up at the end of the schedule
			priority = sys.maxsize if future is None else future.next_run_time()
			newwi = _WorkItem(priority=priority, counter=next(self._counter), 
				future=future, deleted=False)
			heapq.heappush(self._queue, newwi)
			if future is not None:
				self._index[future._id] = newwi
			if empty or newwi < nextwi:
				self._available.notify_all()

	def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[ScheduledFuture]:
		deadline = None if timeout is None else time.time() + timeout
		with self._available:
			while True:
				rem_til_deadline = None if deadline is None else time.time() - deadline
				if rem_til_deadline is not None and rem_til_deadline <= 0:
					raise Empty
				
				if not self._queue:
					if not block:
						raise Empty
					self._available.wait(rem_til_deadline)
					continue
				
				nextwi = self._queue[0]
				
				if nextwi.deleted:
					heapq.heappop(self._queue)
					if self._queue:
						self._available.notify_all()
					continue
				
				rem_til_runnable = None
				if nextwi.future is not None:
					rem_til_runnable = nextwi.future.next_run_time() - time.time()
				
				if nextwi.future is None or rem_til_runnable <= 0:
					heapq.heappop(self._queue)
					if nextwi.future is not None:
						del self._index[nextwi.future._id]
					if self._queue:
						self._available.notify_all()
					return nextwi.future

				sleep_secs = rem_til_runnable
				if rem_til_deadline is not None:
					sleep_secs = min(rem_til_runnable, rem_til_deadline)
				self._available.wait(sleep_secs)
	
	def get_nowait(self) -> Union[ScheduledFuture, None]:
		return self.get(block=False)
	
	def delete(self, id: str) -> bool:
		# note that the item is only marked as deleted but still remains in the queue
		# this was done to achieve O(1) deletion time instead of O(log n) to actually
		# remove the future. refs to the future are removed, so minimal memory is used
		# until the future is actually processed and removed from the queue
		with self._available:
			# remove the future from the index so that if it is rescheduled
			# the same id can be used again without conflicting
			try:
				wi = self._index.pop(id)
			except KeyError:
				return False
			wi.future = None  # remove ref to avoid keeping callable,args,kwargs alive
			wi.deleted = True  # so queue will skip over it when reached
			self._available.notify_all()  # wake up waiters to avoid waiting
			# while shutting down
			return True


class ScheduledThreadPoolExecutor(ThreadPoolExecutor):

	def __init__(self, late_run_limit: float = 1, 
		daemon: bool = False, *args, **kwargs):
		"""Init scheduled thread pool executor.
		
		Args:
			late_run_limit: Optional. Max number of seconds of lag
				between when a future was scheduled to run and when 
				it was actually run, above which a warning will be logged,
				indicating you may need more workers to do the work on time.
				Defaults to 1 second.
			daemon: Optional. True if you want to use daemon threads, in which
				case the executor will not wait for them to complete. The standard 
				implementation does not allow this because it can sometimes
				create undefined states for the interpreter, but there are some
				use-cases where it is valid/useful. Defaults to False, matching
				with the standard library.
			max_workers: The maximum number of threads that can be used to
				execute the given calls.
			thread_name_prefix: An optional name prefix to give our threads.
			initializer: A callable used to initialize worker threads.
			initargs: A tuple of arguments to pass to the initializer.
		"""
		super().__init__(*args, **kwargs)
		self._work_queue = _ScheduledFutureQueue()
		self.late_run_limit = late_run_limit
		self.daemon = daemon

	def _adjust_thread_count(self):
		# if idle threads are available, don't spin new threads
		if self._idle_semaphore.acquire(timeout=0):
			return

		# When the executor gets lost, the weakref callback will wake up
		# the worker threads.
		def weakref_cb(_, q=self._work_queue):
			q.put(None)

		num_threads = len(self._threads)
		if num_threads < self._max_workers:
			thread_name = '%s_%d' % (self._thread_name_prefix or self,
									 num_threads)
			t = threading.Thread(name=thread_name, target=_worker,
								 args=(weakref.ref(self, weakref_cb),
									   self._work_queue,
									   self._initializer,
									   self._initargs))
			t.daemon = self.daemon
			t.start()
			self._threads.add(t)
			# do not put the thread in the collection so we can skip
			# waiting for it on interpreter shutdown
			if not t.daemon:
				_threads_queues[t] = self._work_queue

	def _schedule(self, future):
		with self._shutdown_lock, _global_shutdown_lock:
			if self._broken:
				raise BrokenThreadPool(self._broken)
			if self._shutdown:
				raise RuntimeError('cannot schedule new futures after shutdown')
			if _shutdown:
				raise RuntimeError('cannot schedule new futures after '
								   'interpreter shutdown')
			self._work_queue.put(future)
			self._adjust_thread_count()
			return future

	def schedule(self, fn: Callable, /, args: Optional[Tuple[Any]] = None, 
		kwargs: Optional[Dict[str, Any]] = None, weak: Optional[bool] = None,
		context: Optional[contextvars.Context] = None,
		period: float = 0, start: Optional[float] = None, 
		name: Optional[str] = None) -> ScheduledFuture:
		"""Schedule a callable to run and return a `ScheduledFuture`.
		
		Args:
			fn: The callable or a weakref to a callable to execute.
			args: Tuple of arguments to `fn`
			kwargs: Mapping of key-value arguments to `fn`
			weak: Optional. If True, a weakref to the `fn` will be created and stored
				in the future instead of `fn` itself. If False, `fn` will be stored as-is.
				If None and `fn` is a function, lambda functino, or weakref,
				`fn` will be stored as-is; otherwise, a weakref to it will be stored.
				If `fn` is a weakref, it will be stored as-is, regardless of this argument.
				Defaults to None.
			context: Optional. The `contextvars.Context` in which to run `fn`.
				Defaults to None, which runs in the current context.
			period: Optional. The number of seconds to delay between runs of this
				future. If 0, the future will run only once and can be treated like
				a regular `Future`.
			start: Optional. Epoch time when the future should first start executing.
				Defaults to None, which starts immediately.
			name: Optional. A name identifying the future. It does not need to be unique.

		Returns:
			ScheduledFuture
		"""
		future = ScheduledFuture(pool=self, fn=fn, args=args, kwargs=kwargs, 
			weak=weak, start=start, period=period, name=name, context=context)
		return self._schedule(future)

	def _delete(self, future: ScheduledFuture) -> bool:
		return self._work_queue.delete(future._id)

	def reschedule(self, future: ScheduledFuture, start: Optional[float] = None,
		period: Optional[float] = None) -> ScheduledFuture:
		"""Reschedule a `ScheduledFuture`
		
		The old future will first be cancelled and waited for before a
		copy is created and scheduled.

		Args:
			future: The `ScheduledFuture` instance to reschedule.
			start: Optional. Epoch time to start execution of the future.
				Defaults to None which uses the existing start time.
			period: Optional. The number of seconds to delay between execuions
				of the future. Defaults to None, which uses the existing period.
		
		Returns:
			A new `ScheduledFuture` instance, scheduled with the given settings.
		"""
		future.cancel()
		wait([future])
		new_future = ScheduledFuture(
				pool=self,
				fn=future._fn,
				args=future._args,
				kwargs=future._kwargs, 
				weak=None,
				context=future._context,
				period=period or future._period,
				start=start,
				name=future._name,
			)
		new_future._runtime = future._runtime
		new_future._executions = future._executions
		new_future._exceptions = future._exceptions
		self._schedule(new_future)
		return new_future

	def submit(self, fn: Callable, /, *args: Any, **kwargs: Any) -> ScheduledFuture:
		"""Submit a function to run immediately, just like any executor."""
		return self.schedule(fn, args, kwargs)

	def _initializer_failed(self):
		with self._shutdown_lock:
			self._broken = ('A thread initializer failed, the thread pool '
							'is not usable anymore')
			# Drain work queue and mark pending futures failed
			while True:
				try:
					future = self._work_queue.get_nowait()
				except Empty:
					break
				if future is not None:
					future.set_exception(BrokenThreadPool(self._broken))

	def shutdown(self, wait=True, *, cancel_futures=False):
		with self._shutdown_lock:
			self._shutdown = True
			if cancel_futures:
				# Drain all work items from the queue, and then cancel their
				# associated futures.
				while True:
					try:
						future = self._work_queue.get_nowait()
					except Empty:
						break
					if future is not None:
						future.cancel()

			# Send a wake-up to prevent threads calling
			# _work_queue.get(block=True) from permanently blocking.
			self._work_queue.put(None)
		if wait:
			for t in self._threads:
				t.join()
	shutdown.__doc__ = Executor.shutdown.__doc__
