# mongo-queue

Allows using MongoDB as a persistent queue of sorts, with automatic retry and backoff.

## Why?
Sometimes you have a Mongo instance lying around and you use what you have.

## Features
* Persistent queue
* Configurable failure retry
* In-order processing of records
* Configurable batch size limit to throttle processing
* Configurable processing for records which have failed too many times
* Automatic cleanup of completed records
* Ability to skip a record so it'll processed again later
* Ability to fail a record immediately so it won't be retried
* Hook to reset records that have failed previously

## Example

```js
'use strict';

const timestring = require('timestring');
const api = require('lib/http')('https://api.acme.your-domain.com/updates');
const email = require('lib/email');
const format = require('util').format;

const mongoQueue = require('mongo-queue');

const taskQueue = queue({
  // URL for MongoDB
  mongoUrl: 'mongodb://localhost:27017/localdb',
  
  // Name of the collection that will hold enqueued records
  collectionName: 'task',

  // Processes records every minute using the "cron" module
  processCron: '*/1 * * * *',

  // Processes records every 20 minutes using the "cron" module
  cleanupCron: '*/20 * * * *',

  // Number of records to process for each tick of the process
  batchSize: 200,

  // Max age in ms a processed record can be before getting deleted. We're using timestring 
  // to convert 1 day to the number of milliseconds in a day
  maxRecordAge: timestring('1 day', 'ms'),

  // Max tries before we invoke the "onFailure" function
  retryLimit: 5,

  // Wait 3 minutes after getting an error before the record can be reprocessed.
  // With the default backoffCoefficient of 1.5, this means records will be
  //  retried after 3 minutes, ~8 minutes, ~15 minutes, etc.
  backoffMs: timestring('3 minutes', 'ms'),

  onProcess: function processItem(record) {
    // This will be called for each enqueued record in each batch.
    // It may return a promise or a value. (There is no callback.)
    // The data passed to enqueue() (below) will be in the "data" property. 
    // Other properties include "status", and are generated by mongo-queue.
    // It is NOT recommended that you change these.
    return api.post(record.data);
  },

  onFailure: function processItemFailure(record) {
    // This will be called when a record has been attempted and failed too many times.
    // The record will not be processed again.
    // It may return a promise or a value. (There is no callback.)
    return email({
      subject: 'Failed to process task:' + record._id,
      body: 'Data from client was:\n' + JSON.stringify(record.data, null, 2)
    });
  }
});

// Enqueue a record to be processed
taskQueue.enqueue({
  user: 'MikeyBurkman',
  velocity: [8, 5],
  acceleration: [2, 12],
  position: [40, 50]
});
```

## Behaviors
The goal of this module is to allow an API to quickly accept data from client
devices, but defer processing for a later point in time so that it can be done
in controlled batches (_batchSize_). It will write (_enqueue_) items to the
given _collectionName_ and they will be loaded for each tick for the given cron
tab _processCron_. Each time _processCron_ "ticks" the _onProcess_ function will
be called for each record in _collectionName_ in series.

## API
This module exports a single `mongoQueue` function that is used to create queue instances,
as well as a few utility functions.

```js
var mongoQueue = require('mongo-queue');`
```

### `var queue = mongoQueue(options)`
Creates a new queue instance. Options can contain:

* mongoUrl - MongoDB URL to connect to.
* collectionName - Name for a collection, e.g 'jobs'
* batchSize - Size of a batch to read into memory each tick of the queue
* maxRecordAge - Max age of an entry in milliseconds. If this entry age exceeds this then it will be removed when the cleanup task runs.
* onProcess - Function to invoke for processing a record. Must return a Promise.
* onFailure - Function to invoke when a record fails to process _retryLimit_ times. Must return a Promise.
* retryLimit - Number of times to try process a record before considering it a failure.
* backoffMs - Number of milliseconds to backoff after errors. The ms backoff will increase exponentially according to backoffCoefficient.
Defaults to 0, which means no backoff after errors -- the record will be reprocessed on the next opportunity.
* backoffCoefficient - Number for exponentially increasing the backoff time after errors. Defaults to 1.5.
* processCron - Cron tab used to determine when to process batches.
* cleanupCron - Cron tab used to determine when to clean stale data.
* continueProcessingOnError - Set to true to continue processing records even one fails. This will allow for higher throughput, but also allows records to be processed out of order when things fail. Defaults to false. (Defaults to standard queue behavior.)

### `queue.enqueue(obj[, callback])`
Add a new Object to the queue for processing. Returns a Promise if a callback
is not supplied. Rejects if writing to MongoDB fails.

### `queue.processNextBatch([callback])`
Immediately start processing the next batch of items without waiting for a
"tick" of the job. Calls callback or resolves a returned Promise once complete.
Will have no effect if called when a batch is currently processing.

### `queue.cleanup([callback])`
Immediately invoke the clean up task to remove records older than
_maxRecordAge_. If this cleanup is already running then this has no effect.
Accepts a callback, or returns a promise to indicate completion.

### `queue.resetRecords(recordIDs[, callback])`
Resets records to their initial received state, and they will be ready to proccess immediately.
This is useful for reprocessing certain previously-failed records, after the issue causing them to
fail has been resolved. `records` is an array of either ID strings, or ObjectIds.

## Skipping records
The mongoQueue function has a `skip(backoffTime)` function attached to it, which can be used like so:
```js
var mongoQueue = require('mongo-queue');

...

  onProcess: function(record) {
    if (record.sequenceID > currSequenceID) {
      throw mongoQueue.skip(100);
      // Alternatively, you can also return a rejected Promise:
      return Promise.reject(mongoQueue.skip(100)); // Slightly more efficient than throwing
    }

    // Else continue processing
  }
```
This will set the record's status in Mongo to `'skipped'`. This will not count as an error, or affect the retryCount.

The argument is the number of milliseconds to wait before trying to process again. 
If not provided, it defaults to 0ms -- it will be eligible for the next batch.

Note that skipping a record will mean that record will NOT be processed in the insertion order. For instance, if the the queue contains records ['A', 'B', 'C'], and 'A' is skipped, the order of processing could then be ['B', 'C', 'A'].

## Failing records
The mongoQueue function has a `fail(reason)` function attached to it, which can be used like so:
```js
var mongoQueue = require('mongo-queue');

...

  onProcess: function(record) {
    try {
      validate(record);
    } catch (validationErr) {
      // If it fails validation, no use retrying, so just fail()
      throw mongoQueue.fail(validationErr);
      // Alternatively, you can also return a rejected Promise:
      return Promise.reject(mongoQueue.fail(validationErr)); // Slightly more efficient than throwing
    }

    // Continue processing
  }
```
This will fail the record immediately. It will not be retried, and it will be passed to
the `onError` handler. The record will get the additional property of `immediateFailure` set to `true`.
Its `failureReason` value will be set to the `reason` argument.

## Caveats
* Not multi-thread safe. Having multiple processes or servers running the queue processing code on the same collection can lead to duplicate processing of records.
