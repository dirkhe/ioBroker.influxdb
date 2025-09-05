"use strict";

class Database {
  connect() {}
  getDatabaseNames(callback) {}
  createDatabase(dbname, callback_error) {}
  createRetentionPolicyForDB(dbname, retention, callback_error) {}
  dropDatabase(dbname, callback_error) {}

  writeSeries(series, callback_error) {}
  writePoints(seriesId, pointsToSend, callback_error) {}
  writePoint(seriesName, values, options, callback_error) {}

  query(query, callback) {}

  calculateShardGroupDuration(retentionTime) {
    // in seconds
    // Shard Group Duration according to official Influx recommendations
    if (!retentionTime) {
      // infinite
      return 604800; // 7 days
    } else if (retentionTime < 172800) {
      // < 2 days
      return 3600; // 1 hour
    } else if (retentionTime >= 172800 && retentionTime <= 15811200) {
      // >= 2 days, <= 6 months (~182 days)
      return 86400; // 1 day
    } else {
      // > 6 months
      return 604800; // 7 days
    }
  }

  async queries(queries, callback) {
    const collectedRows = [];
    let success = false;
    let errors = [];
    for (const query of queries) {
      await new Promise((resolve, reject) => {
        this.query(query, (error, rows) => {
          if (error) {
            errors.push(error);
            collectedRows.push([]);
          } else {
            success = true;
            collectedRows.push(rows);
          }
          resolve();
        });
      });
    }
    let retError = null;
    if (errors.length) {
      retError = new Error(
        `${errors.length} Error happened while processing ${queries.length} queries`
      );
      retError.errors = errors;
    }
    callback(retError, success ? collectedRows : null);
  }
}

/* Old node-influx lib had some connection-pool handling that is not present in new influx lib,
   so to not break old code we use a fictional pool here. */
class FakeConnectionPool {
  constructor() {
    this.connections = [];
  }

  activatePool() {
    this.connections.push("fakehost");
  }

  getHostsAvailable() {
    return this.connections;
  }
}

module.exports = {
  Database: Database,
  FakeConnectionPool: FakeConnectionPool,
};
