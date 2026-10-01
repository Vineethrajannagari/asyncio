

from collections import deque  # deque gives O(1) append and popleft, ideal for a FIFO queue

PENDING = "PENDING"  # state label: result not available yet
DONE = "DONE"  # state label: result is available


class Future:  # a result that may not exist yet
    def __init__(self, loop):  # every Future belongs to one event loop
        self.loop = loop  # keep the loop so we can schedule callbacks on it
        self._state = PENDING  # a new Future always starts out pending
        self._result = None  # placeholder until set_result is called
        self._callbacks = []  # functions to run once the Future completes

    def done(self):  # is the result available?
        return self._state == DONE  # True only after set_result

    def result(self):  # fetch the result
        if self._state != DONE:  # can't return something that doesn't exist yet
            raise RuntimeError("Future is not done yet")  # required: error if still pending
        return self._result  # hand back the stored value

    def set_result(self, value):  # complete the Future with a value
        if self._state == DONE:  # completing twice is a bug
            raise RuntimeError("Future is already done")  # required: error on second call
        self._state = DONE  # PENDING -> DONE
        self._result = value  # store the value for later result() calls
        # Callbacks never run inline; they go through the ready queue.
        for cb in self._callbacks:  # go through everyone waiting on this Future
            self.loop.call_soon(cb, self)  # schedule cb(self) to run later, not right now
        self._callbacks.clear()  # callbacks fire only once, so empty the list

    def add_done_callback(self, callback):  # register "call me when finished"
        if self.done():  # already finished, so waiting would hang forever
            self.loop.call_soon(callback, self)  # schedule the callback right away (still via the queue)
        else:  # still pending
            self._callbacks.append(callback)  # remember it; set_result will schedule it

    def __await__(self):  # makes "await future" legal
        if not self.done():  # result not ready, so we must suspend
            # Hand ourselves up to whoever is driving the coroutine (the Task).
            yield self  # suspend; this value pops out of coroutine.send() in Task._step
        return self.result()  # once resumed (or if already done), the await evaluates to the result


class Task(Future):  # a Future whose result comes from running a coroutine
    def __init__(self, coroutine, loop):  # takes the coroutine object and its loop
        super().__init__(loop)  # set up Future state (pending, callbacks, loop)
        self.coroutine = coroutine  # the coroutine this Task will drive

    def _step(self):  # run the coroutine until its next suspension point
        """Advance the coroutine to its next suspension point."""  # docstring
        try:  # send() signals completion via an exception
            yielded = self.coroutine.send(None)  # start or resume the coroutine; returns what it yielded
        except StopIteration as e:  # the coroutine returned
            self.set_result(e.value)  # coroutine finished -> Task is DONE, e.value is its return value
            return  # nothing more to schedule

        if isinstance(yielded, Future):  # the coroutine awaited a pending Future/Task
            # Awaiting a pending Future/Task: resume when it completes.
            yielded.add_done_callback(self._wakeup)  # when it completes, _wakeup will resume us
        elif yielded is None:  # bare yield, which is what yield_control() produces
            # yield_control(): go to the back of the ready queue.
            self.loop.call_soon(self._step)  # reschedule ourselves behind everyone else
        else:  # anything else is unsupported
            raise RuntimeError(f"Task got unexpected yield: {yielded!r}")  # fail loudly

    def _wakeup(self, future):  # callback fired when the awaited Future completes
        self._step()  # resume the coroutine; its await now returns the Future's result


class EventLoop:  # runs callbacks from a FIFO queue
    def __init__(self):  # set up loop state
        self.ready = deque()  # FIFO queue of (callback, args) pairs
        self._running = False  # flag checked by run_forever

    def call_soon(self, callback, *args):  # schedule callback(*args) for later
        self.ready.append((callback, args))  # add to the back; do NOT call it now

    def create_task(self, coroutine):  # wrap a coroutine in a Task and schedule it
        task = Task(coroutine, self)  # 1-3: create Task, store coroutine, attach to this loop
        self.call_soon(task._step)  # 4: schedule the Task's first execution
        return task  # 5: give the Task back to the caller

    def run_forever(self):  # process callbacks until told to stop or out of work
        self._running = True  # mark the loop as running
        while self._running and self.ready:  # keep going while not stopped and work remains
            callback, args = self.ready.popleft()  # take the oldest callback (FIFO)
            callback(*args)  # execute it; it runs to completion, nothing interrupts it
        self._running = False  # loop is no longer running

    def run_until_complete(self, task):  # run the loop until the given Task is done
        def on_done(future):  # named callback, receives the finished Future (our Task)
            self.stop()  # stop the loop once the Task is done
        task.add_done_callback(on_done)  # register it so it fires when the Task finishes
        self.run_forever()  # drive everything
        if not task.done():  # loop drained but the Task never finished (e.g. awaited a Future nobody sets)
            raise RuntimeError("Loop ran out of work before the task finished")  # report it
        return task.result()  # hand back the Task's return value

    def stop(self):  # ask the loop to stop
        self._running = False  # run_forever sees this at the top of its next iteration


class _YieldControl:  # tiny awaitable used by yield_control
    def __await__(self):  # makes instances awaitable
        yield  # yields None -> Task reschedules itself


async def yield_control():  # what user coroutines call: await yield_control()
    await _YieldControl()  # suspends the current coroutine once


# --------------------------------------------------------------------------
# Demos
# --------------------------------------------------------------------------
if __name__ == "__main__":  # only run demos when executed directly
    print("--- Demo 1 ---")  # header for the output
    loop = EventLoop()  # fresh loop for this demo

    async def hello():  # simple coroutine function
        print("hello")  # runs when the Task first steps
        return 42  # becomes the Task's result via StopIteration.value

    print(loop.run_until_complete(loop.create_task(hello())))  # wrap, run to completion, print 42

    print("--- Demo 2 ---")  # header
    loop = EventLoop()  # fresh loop

    async def worker2(name):  # worker that yields once
        print(name, "start")  # runs on first step
        await yield_control()  # pause; go to back of the queue
        print(name, "end")  # runs on second step

    loop.create_task(worker2("A"))  # queue = [A]
    loop.create_task(worker2("B"))  # queue = [A, B]
    loop.run_forever()  # A start, B start, A end, B end

    print("--- Demo 3 ---")  # header
    loop = EventLoop()  # fresh loop

    async def worker3(name):  # worker that yields twice
        print(name, 1)  # step 1
        await yield_control()  # pause
        print(name, 2)  # step 2
        await yield_control()  # pause again
        print(name, 3)  # step 3

    loop.create_task(worker3("A"))  # queue = [A]
    loop.create_task(worker3("B"))  # queue = [A, B]
    loop.run_forever()  # interleaves: A1 B1 A2 B2 A3 B3

    print("--- Demo 4 ---")  # header
    loop = EventLoop()  # fresh loop
    future = Future(loop)  # a pending Future nobody has completed yet

    async def waiter():  # coroutine that waits on the Future
        print("waiting")  # printed before suspending
        result = await future  # pending, so the Task parks itself
        print(result)  # runs after the Future is completed

    loop.create_task(waiter())  # queue = [waiter step]
    loop.call_soon(future.set_result, "done")  # queue = [waiter step, set_result]
    loop.run_forever()  # waiter parks, set_result fires, wakeup runs, prints "done"

    print("--- Demo 5 ---")  # header
    loop = EventLoop()  # fresh loop

    async def calculate():  # inner coroutine
        await yield_control()  # pause once
        return 100  # final result

    async def main():  # outer coroutine that awaits a Task
        task = loop.create_task(calculate())  # schedule the inner Task
        return await task  # suspend until the inner Task is DONE, then return its result

    print(loop.run_until_complete(loop.create_task(main())))  # prints 100

    print("--- Future rules ---")  # header
    loop = EventLoop()  # fresh loop
    f = Future(loop)  # new pending Future
    assert f.done() is False  # starts pending
    try:  # result() before completion must raise
        f.result()  # should fail
    except RuntimeError as e:  # catch the expected error
        print("result() before done ->", e)  # show the message
    f.set_result(42)  # complete it
    assert f.done() and f.result() == 42  # now done with the right value
    try:  # a second set_result must raise
        f.set_result(1)  # should fail
    except RuntimeError as e:  # catch the expected error
        print("second set_result ->", e)  # show the message
