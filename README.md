# Scheduled Futures

concurrent.futures extension with a ScheduledThreadPoolExecutor to handle delayed and periodic futures with rescheduling

## Preamble

[concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) is pretty awesome, but it does not handle periodic work which comes up a fair amount when creating applications.

[apscheduler](https://github.com/agronholm/apscheduler) is great, but trying to wait for jobs to complete involves a racy mess of callbacks.

[asyncio](https://docs.python.org/3/library/asyncio.html) is also good and can simply handle periodic work with a `asyncio.sleep` call, but this is more trouble than it is worth when most of your codebase uses system calls / dlls / synchronous libraries you dont want to rewrite or coat with `asyncio.to_thread` calls.

This package was created to solve this problem. Please see [Features](#features) below.

Documentation is just what there is in this README and the code itself.

## Table of Contents

- [Scheduled Futures](#scheduled-futures)
  - [Preamble](#preamble)
  - [Table of Contents](#table-of-contents)
  - [Inspiration](#inspiration)
  - [Features](#features)
  - [Technologies](#technologies)
  - [Install](#install)
  - [Development](#development)
  - [Example](#example)
    - [Code](#code)
    - [Output](#output)

## Inspiration

This package was inspired by [this feature request](https://github.com/python/cpython/issues/62156) and some of the code there.

## Features

- Schedule futures at a fixed delay using a thread pool executor
- Rescheduling of `ScheduledFuture` instances after they have started
- `ScheduledThreadPoolExecutor` is a subclass of `ThreadPoolExecutor`, so it can be used anywhere `ThreadPoolExecutor` can, such as for the asyncio default executor
- Logged warnings when a `ScheduledFuture's` runtime exceeded its period, indicating it may be scheduled to run too quickly
- Logged warnings when a `ScheduledFuture` is run later scheduled, indicating you may need more workers to keep up
- Statistics on the number of executions / exceptions / total runtime / average runtime for each `ScheduledFuture`
- Supports daemon usage, so you can handle things like system calls which cannot be aborted without the process hanging until they complete.

## Technologies

- Python >= 3.5

## Install

```python
pip install scheduled_futures
```

## Development

Pull requests and new feature requests are welcome.

## Example

### Code

```python
import time
from concurrent.futures import wait, CancelledError
import logging

from scheduled_futures import ScheduledThreadPoolExecutor

logging.basicConfig()  # to see warning messages from lib
# which are pumped into a null handler by default


def work():
	print(time.time(), 'work!')


def slow_work():
	time.sleep(0.25)
	print(time.time(), 'slow work...huuh')


with ScheduledThreadPoolExecutor() as stpool:
	stpool.submit(work)  # same interface as regular ThreadPoolExecutor still works

	# simple demo of scheduling a callable a little later
	print('\nDelayed future example')
	print(time.time(), 'work scheduled')
	future = stpool.schedule(work, start=time.time() + 0.5)
	time.sleep(1)

	# simple demo of scheduling a callable periodically
	print('\nPeriodic future example')
	future = stpool.schedule(work, period=0.25)
	time.sleep(1)
	future.cancel()
	wait([future])

	# show log warning for each execution because the future
	# ran for longer than the period between runs
	print('\nPeriod too short for long-running future example')
	future = stpool.schedule(slow_work, period=0.20)
	time.sleep(1)
	future.cancel()
	wait([future])


with ScheduledThreadPoolExecutor() as stpool:
	print('\nRescheduling work example')
	future = stpool.schedule(work, period=0.25)
	time.sleep(0.55)
	print('Rescheduling')
	stpool.reschedule(future, period=1)
	time.sleep(2)


def slow_work2():
	time.sleep(0.15)
	print(time.time(), 'slow work2...huuuuuuh')


with ScheduledThreadPoolExecutor(late_run_limit=0.1, max_workers=1) as stpool:

	# show log warning before some executions because there are not enough workers
	# to keep up with the execution schedule
	print('\nNot enough workers example')
	futures = []
	for _ in range(3):
		futures.append(stpool.schedule(slow_work2, period=0.20))
	time.sleep(1)
	list(map(lambda f: f.cancel(), futures))
	wait(futures)


i = 0
def cancelled_work():
	global i
	i += 1
	if i >= 3:
		raise CancelledError('Stop working, now!')
	print(time.time(), 'ran without cancellation')


with ScheduledThreadPoolExecutor() as stpool:

	# cancel a periodic from inside a periodic
	print('\nCancel from inside callable example')
	future = stpool.schedule(cancelled_work, period=0.25)
	try:
		future.result()
	except CancelledError:
		print('work cancelled!')
```

### Output

```text
1654566752.6947718 work!

Delayed future example
1654566752.6957695 work scheduled
1654566753.2106197 work!

Periodic future example
1654566753.700693 work!
1654566753.9519553 work!
1654566754.2048206 work!
1654566754.4566417 work!
1654566754.7094283 work!

Period too short for long-running future example
1654566754.963385 slow work...huuh
WARNING:scheduled_futures:Periodic scheduled future runtime exceeded period.
1654566755.4197686 slow work...huuh
WARNING:scheduled_futures:Periodic scheduled future runtime exceeded period.
1654566755.8865035 slow work...huuh
WARNING:scheduled_futures:Periodic scheduled future runtime exceeded period.

Rescheduling work example
1654566755.8884244 work!
1654566756.1499033 work!
1654566756.4140623 work!
Rescheduling
1654566756.4449573 work!
1654566757.454106 work!
1654566758.462207 work!

Not enough workers example
1654566758.6189985 slow work2...huuuuuuh
WARNING:scheduled_futures:Late to run scheduled future.
1654566758.7743778 slow work2...huuuuuuh
WARNING:scheduled_futures:Late to run scheduled future.
1654566758.932357 slow work2...huuuuuuh
WARNING:scheduled_futures:Late to run scheduled future.
1654566759.0854318 slow work2...huuuuuuh
WARNING:scheduled_futures:Late to run scheduled future.
1654566759.2414527 slow work2...huuuuuuh
WARNING:scheduled_futures:Late to run scheduled future.
1654566759.3976445 slow work2...huuuuuuh
WARNING:scheduled_futures:Late to run scheduled future.
1654566759.5528805 slow work2...huuuuuuh

Cancel from inside callable example
1654566759.554882 ran without cancellation
1654566759.8188088 ran without cancellation
work cancelled!
```
