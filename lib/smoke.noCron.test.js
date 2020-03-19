'use strict';
var Promise = require('bluebird');
var sinon = require('sinon');
var expect = require('expect');
require('sinon-as-promised')(Promise);

describe(__filename, function() {
  var COLLECTION_NAME = 'SMOKE_TEST_RECORDS';
  var db = require('./db')(process.env.QUEUE_TEST_URL);

  function getDb() {
    return db.getCollection(COLLECTION_NAME);
  }

  function getAllRecords() {
    return getDb().then(function(c) {
      return c.find().toArray();
    });
  }

  function removeAll() {
    return getDb().call('remove');
  }

  beforeEach(removeAll);

  it('Should smoke test with Cron disabled', function() {
    process.env.NODE_MASTER_CRON = 'false';
    var mongoQueue = require('./index');
    this.timeout(15000); // This thing could run for a little bit

    var processStub = sinon.stub().resolves();

    var queue = mongoQueue({
      mongoUrl: process.env.QUEUE_TEST_URL,
      collectionName: COLLECTION_NAME,
      processCron: '*/5 * * * * *', // Every 5 seconds
      onProcess: processStub
    });

    queue.enqueue({
      noCronfoo: true
    });
    queue.enqueue({
      noCronbar: true
    });

    // Wait long enough for it to process
    return Promise.delay(6000)
      .then(getAllRecords)
      .then(function(records) {
        expect(records.length).toEqual(2);
        expect(records[0]).toInclude({ status: 'received' });
        expect(records[1]).toInclude({ status: 'received' });

        expect(processStub.callCount).toEqual(0);
      })
      .then(function() {
        // Submit another record and make sure we process again
        return queue.enqueue({
          noCronbaz: true
        });
      })
      .then(function() {
        return Promise.delay(6000);
      })
      .then(getAllRecords)
      .then(function(records) {
        expect(records.length).toEqual(3);
        expect(records[0]).toInclude({ status: 'received' });
        expect(records[1]).toInclude({ status: 'received' });
        expect(records[2]).toInclude({ status: 'received' });

        expect(processStub.callCount).toEqual(0);
      });
  });
});
