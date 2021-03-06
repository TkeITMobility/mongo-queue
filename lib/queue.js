'use strict';

// Retry queue logic using Mongo

const Promise = require('bluebird');
const queueDb = require('./db');
const utils = require('./utils');

const STATUS_CODES = {
  received: 'received',
  processed: 'processed',
  failed: 'failed',
  skipped: 'skipped',
  notified: 'notified',
  notifyFailure: 'notifyFailure'
};

const STOP_PROCESSING = new Error('____stopProcessing____');

/**
 * Creates a batch processor/uploader instance that can be used to trigger a
 * job on a set schedule. The purpose of this is to write client data to a
 * temporary table before sending to a system of record. This improves response
 * times for clients, and places the retry responsibility on the cloud
 * application meaning less battery and bandwith used by a client.
 *
 * @param  {Object} opts
 * @return {Object}
 */
module.exports = function(opts) {
  const collectionName = opts.collectionName;
  const batchSize = opts.batchSize;
  const maxRecordAge = opts.maxRecordAge;
  const onProcess = opts.onProcess;
  const onFailure = opts.onFailure;
  const onPreHook = opts.onPreHook;
  const retryLimit = opts.retryLimit;
  const backoffMs = opts.backoffMs;
  const backoffCoefficient = opts.backoffCoefficient || 1.5;
  const continueProcessingOnError = opts.continueProcessingOnError;
  const db = queueDb(opts.mongoUrl);
  const onStatusesCheckProcess = opts.onStatusesCheckProcess;

  ensureIndexes().catch((err) =>
    console.error('Error ensuring mongo-queue indexes', err.stack || err)
  );

  return {
    enqueue: enqueue,
    processNextBatch: processNextBatch,
    cleanup: cleanup,
    resetRecords: resetRecords,
    statusesCheck: statusesCheck
  };

  /**
   * Add an item to the queue for processing.
   *
   * Callback is called or Promise is resolved when it has been written to
   * MongoDB for processing in the future.
   *
   * @param  {Object}   record The data from a client or other function
   * @param  {Function} cb
   * @return {Promise}
   */
  function enqueue(record, cb) {
    return insertNewRecord(record).asCallback(cb);
  }

  /**
   * Query for everything in the given collection with status [received, failed]
   * and try to process them using the onProcess function provided to our
   * original opts Object
   *
   * Procesing occurs in series.
   *
   * The returned promise resolves when all items are processed, and rejected
   * if a failire occurs.
   *
   * @return {Promise}
   */
  function processNextBatch(callback) {
    return Promise.resolve()
      .then(() => {
        if (onPreHook) {
          return onPreHook();
        } else {
          return Promise.resolve();
        }
      })
      .then(getNextBatch)
      .mapSeries(processRecord)
      .catch(function(err) {
        // The error might just be that we needed to stop processing early on purpose.
        // In which case, don't propagate it to the user.
        if (err !== STOP_PROCESSING) {
          throw err;
        }
      })
      .asCallback(callback);
  }

  /**
   * Deletes any records with status=processed and a processedDate older than
   * the given maxRecordAge
   *
   * @return {Promise}
   */
  function cleanup(callback) {
    return Promise.resolve()
      .then(getCollection)
      .then(function(collection) {
        const minDate = new Date(Date.now() - maxRecordAge);
        return collection.remove({
          status: STATUS_CODES.processed,
          processedDate: {
            $lte: minDate
          }
        });
      })
      .asCallback(callback);
  }

  function getCollection() {
    return db.getCollection(collectionName);
  }

  function insertNewRecord(record) {
    return getCollection()
      .then(function(collection) {
        const data = {
          receivedDate: new Date(),
          status: STATUS_CODES.received,
          available: new Date(), // Available immediately
          data: record
        };

        return collection.insert(data);
      })
      .then(function(result) {
        return result.ops[0]; // Returns the newly-inserted object
      });
  }

  function processRecord(record) {
    // If retryLimit is negative, then we'll retry forever
    if (recordHasFailed(record)) {
      return notifyFailedRecord(record);
    } else {
      return Promise.resolve()
        .then(function() {
          return onProcess(record);
        })
        .then(function(additionalInfo) {
          if (additionalInfo && Object.keys(additionalInfo).length > 0) {
            record.additionalInfo = additionalInfo;
          }
          return processSuccess(record);
        })
        .catch(function(err) {
          if (utils.isSkip(err)) {
            return processSkip(record, err);
          } else if (utils.isFail(err)) {
            return failImmediately(record, err);
          } else {
            return processFailure(record, err).then(function() {
              if (!continueProcessingOnError) {
                throw STOP_PROCESSING;
              }
            });
          }
        });
    }
  }

  function recordHasFailed(record) {
    return retryLimit >= 0 && record.retryCount && record.retryCount >= retryLimit;
  }

  function getNextBatch() {
    const getUnprocessed = {
      status: {
        $in: [STATUS_CODES.received, STATUS_CODES.failed, STATUS_CODES.skipped]
      },
      available: {
        $lte: new Date() // Can be processed
      }
    };

    let query;
    if (continueProcessingOnError) {
      query = getUnprocessed;
    } else {
      // If we need to process the next failed record before anything else, then
      //  we need also to bring back all failed records, regardless of availiable date.
      query = {
        $or: [
          {
            status: STATUS_CODES.failed
          },
          getUnprocessed
        ]
      };
    }

    return getCollection()
      .then(function(collection) {
        return collection
          .find(query)
          .sort({
            receivedDate: 1
          })
          .limit(batchSize)
          .toArray();
      })
      .then(prioritizeRecords);
  }

  function getAllStatusesCount() {
    const aggregationQuery = [
      {
        $group: { _id: '$status', count: { $sum: 1 } }
      },
      {
        $project: {
          status: '$_id',
          count: 1,
          _id: 0
        }
      }
    ];

    return getCollection().then(function(collection) {
      return collection.aggregate(aggregationQuery).toArray();
    });
  }

  function prioritizeRecords(records) {
    if (continueProcessingOnError) {
      // Just process this batch, no need to prioritize
      return records;
    }

    // If not continueProcesssingOnError, then we need to make sure that the failed
    //  record is the first to be processed each batch.
    // (Should only be one...)
    const firstFailed = records.find((record) => record.status === STATUS_CODES.failed);

    if (firstFailed && firstFailed.available > new Date()) {
      // If there's a failed record, and it's not available, we can't process anything
      return [];
    } else {
      // This shouldn't matter, but just in case we have multiple failed records, we filter out
      //  unavailible ones.
      return records.filter(function(record) {
        return record.available <= new Date();
      });
    }
  }

  function processSuccess(record) {
    return getCollection().then(function(collection) {
      let set = {
        status: STATUS_CODES.processed,
        processedDate: new Date()
      };

      if (record.additionalInfo) {
        set.additionalInfo = record.additionalInfo;
      }

      return collection.update(
        {
          _id: record._id
        },
        {
          $set: set,
          $unset: {
            failureReason: '',
            retryCount: '',
            available: ''
          }
        }
      );
    });
  }

  function processFailure(record, err) {
    const delay = getErrorBackoffMs(record);

    return getCollection().then(function(collection) {
      return collection.update(
        {
          _id: record._id
        },
        {
          $set: {
            status: STATUS_CODES.failed,
            processedDate: new Date(),
            failureReason: (err && err.stack) || err,
            available: new Date(Date.now() + delay)
          },
          $inc: {
            retryCount: 1
          }
        }
      );
    });
  }

  function processSkip(record, skipErr) {
    return getCollection().then(function(collection) {
      return collection.update(
        {
          _id: record._id
        },
        {
          $set: {
            status: STATUS_CODES.skipped,
            processedDate: new Date(),
            available: new Date(Date.now() + utils.getSkipBackoff(skipErr))
          }
        }
      );
    });
  }

  function notifyFailedRecord(record) {
    return Promise.resolve().then(() => onFailure(record)).reflect().then(function(reflect) {
      const update = {
        processedDate: new Date()
      };

      if (reflect.isRejected()) {
        // If the onFailure() call fails, then, well... not sure what to do...
        // For now we'll log it and set the state to something to indicate the failure.
        const reason = reflect.reason();
        const err = reason.stack || reason;
        console.error('[MONGO-QUEUE] Failure in calling onFailure();', err);
        update.status = STATUS_CODES.notifyFailure;
        update.notifyFailureReason = err;
      } else {
        update.status = STATUS_CODES.notified;
      }

      return getCollection().then(function(collection) {
        return collection.update(
          {
            _id: record._id
          },
          {
            $set: update
          }
        );
      });
    });
  }

  function failImmediately(record, err) {
    const reason = utils.getFailReason(err);
    return getCollection()
      .then(function(collection) {
        return collection
          .update(
            {
              _id: record._id
            },
            {
              $set: {
                status: STATUS_CODES.failed,
                immediateFailure: true,
                failureReason: (reason && reason.stack) || reason
              }
            }
          )
          .then(() => collection.findOne({ _id: record._id }));
      })
      .then((updatedRecord) => notifyFailedRecord(updatedRecord));
  }

  function getErrorBackoffMs(record) {
    const retryCount = record.retryCount || 0;

    if (retryCount === retryLimit) {
      // If we've reached our limit, then don't delay before it gets reprocessed for failure
      return 0;
    }

    return Math.pow(retryCount + 1, backoffCoefficient) * backoffMs;
  }

  function resetRecords(recordIds, cb) {
    return Promise.resolve()
      .then(() => {
        // Make sure all the IDs are ObjectIDs
        const ids = recordIds.map((id) => {
          if (!(id instanceof db.ObjectID)) {
            return new db.ObjectID(id);
          } else {
            return id;
          }
        });

        return getCollection().then(function(collection) {
          return collection
            .update(
              {
                _id: {
                  $in: ids
                }
              },
              {
                $set: {
                  status: STATUS_CODES.received,
                  receivedDate: new Date(),
                  available: new Date(),
                  resetDate: new Date()
                },
                $unset: {
                  processedDate: '',
                  failureReason: '',
                  retryCount: '',
                  immediateFailure: '',
                  notifyFailureReason: ''
                }
              },
              {
                multi: true
              }
            )
            .then((res) => res.result.n);
        });
      })
      .asCallback(cb);
  }

  function ensureIndexes() {
    return getCollection().then((coll) => coll.ensureIndex('status'));
  }

  function statusesCheck() {
    return Promise.resolve().then(getAllStatusesCount).then(function(statusesArray) {
      return onStatusesCheckProcess(statusesArray);
    });
  }
};
