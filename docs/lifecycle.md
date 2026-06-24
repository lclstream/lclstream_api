# Transfer Lifecycle Management

There is a lot of state tracking and management associated with each transfer:

* PortEntry: tracking the user and port pair allocated to the transfer.

  These track a finite resource (ports on the forwarder host), and
  need to be cleared.  Clearing happens by calling the PortUsage
  table's delete(eid) method.  We implement that in the `on_complete`
  callback within `xfer_db.Database.delete` (not currently used anywhere)
  and `Transfer.cancel`.  After calling once, it it set to None,
  preventing double-free.

* Transfer: tracking state of each ClientName (producer, cache=forwarder, user) and logging all calls to Transfer.transition

  Note Transfer.state *could* be reconstructed from replaying all transitions in-sequence but there are two caveats: First, any timeouts (not implemented) would need to be sure not to fire automatically during replay (extremely likely this is the case), and second, transition's own auto-generated "cancel" state transitions need to be double-checked.  Replaying may lead to double-cancellation (as transition generates a transition, and there is already a cancel transition).

* `producer_job`: psik.Job referencing producer
  - filesystem-backed, can be reloaded from job.stamp

* `forwarder_job`: psik.Job referencing producer
  - filesystem-backed, can be reloaded from job.stamp


## Transfer Creation

Producer and Forwarder jobs are now created and managed the same way:

1. POST /transfers calls transfer_mgr.create_transfer, which returns two psik.Job-s (producer_job and forwarder_job), along with the xfer object (Transfer finite state machine)

2. The transfers post adds both job.submit() calls to its backgroundTasks and then returns TransferStatus to the caller

3. As jobs go through their lifecycle, they POST transitions to /callbacks/{producer,forwarder}, which invokes xfer.transition()

4. That transition() returns a callback if there are tasks to do (e.g. job.cancel()).


## Transfer Completion

Transfers should complete in the "normal" sequence:

1. User reads all data from the forwarder

2. Producer detects "all data sent" condition, closes PUSH socket
   and fires a JobState.completed callback to LCLStream-api.

3. LCLStream-api receives the callback and starts a completion
   timer - auto-canceling the forwarder after 10 minutes.

   **Timer is not implemented - see TODO comment.** Right now,
   it forcibly cancels the forwarder.

4. Forwarder detects "all data sent" condition, closes PUSH socket
   and fires a JobState.completed callback to LCLStream-api.

5. LCLStream-api receives this callback and calls `on_complete`.
   Nobody needed to be canceled.

   - The PortEntry's entry in the PortUsage table was deleted
     by `on_complete`.

   - The Transfer's entry in the Database entry remains as record,
     but a log-rotation is needed to clear memory periodically!

     Without calling Database.delete(), the Database.jobids index
     (from psik.Job.stamp-s to eid-s) will grow too.

     Production note: psik.manager.rm(jobid) should be used to
     clean up the file store backing a jobid.

     **Rotation Not implemented**

