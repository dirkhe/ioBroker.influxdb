"use strict";

const Database = require("./Database.js");
const InfluxClient = require("@influxdata/influxdb3-client");

class DatabaseInfluxDB3x extends Database.Database {
  constructor(
    log,
    host,
    port,
    protocol,
    path,
    token,
    database,
    requestTimeout
  ) {
    super();

    this.log = log;

    this.host = host;
    this.port = port || 8181;
    this.protocol = protocol;
    this.path = path;
    this.token = token;
    this.database = database;
    //this.timePrecision = timePrecision; // ms
    this.requestTimeout = requestTimeout; // 30000
    this.request = new Database.FakeConnectionPool();
    //disable ping
    this.ping = false;

    this.connect();
  }

  connect() {
    const url = `${this.protocol}://${this.host}:${this.port}/${
      this.path || ""
    }?database=${this.database}&token=${this.token}`;

    this.log.debug(`Connect InfluxDB3: ${url.slice(0, -50)}`);

    this.connection = new InfluxClient.InfluxDBClient(url);
    this.request.activatePool();
  }

  async getDatabaseNames(callback) {
    try {
      const queryResult = await this.connection.query(
        "SELECT key FROM system.influxdb_schema",
        this.database
      );

      const foundDatabases = [];
      foundDatabases.push(this.database);

      callback(null, foundDatabases);
    } catch (error) {
      callback(error, null);
    }
  }

  getRetentionPolicyForDB(dbname, callback) {
    callback(err, null); // only needed for InfluxQL
  }

  async applyRetentionPolicyToDB(dbname, retention, callback) {
    return callback("RetentionPolicy is only needed for InfluxQL");
  }

  createDatabase(dbname, callback_error) {
    this.log.error("createDatabase is not supported for 3.x");
    callback_error(true);
  }

  dropDatabase(dbname, callback_error) {
    this.log.error("dropDatabase is not supported for 3.x");
    callback_error(true);
  }

  async writeSeries(series, callback_error) {
    this.log.debug(`Write series: ${JSON.stringify(series)}`);

    const points = [];
    for (const [pointId, valueSets] of Object.entries(series)) {
      valueSets.forEach((values) => {
        values.forEach((value) => {
          points.push(this.stateValueToPoint(pointId, value));
        });
      });
    }

    try {
      await this.connection.write(points);
      this.log.debug(`Points written to ${this.database}`);
      callback_error();
    } catch (error) {
      callback_error(error);
    }
  }

  async writePoints(pointId, pointsToSend, callback_error) {
    this.log.debug(
      `Write Points: ${pointId} pointstoSend:${JSON.stringify(pointsToSend)}`
    );

    const points = [];
    pointsToSend.forEach((values) => {
      values.forEach((value) => {
        points.push(this.stateValueToPoint(pointId, value));
      });
    });

    try {
      await this.connection.write(points);
      this.log.debug(`Points written to ${this.database}`);
      callback_error();
    } catch (error) {
      callback_error(error);
    }
  }

  async writePoint(pointId, values, options, callback_error) {
    this.log.debug(
      `Write Point: ${pointId} values:${JSON.stringify(
        values
      )} options: ${JSON.stringify(options)}`
    );

    try {
      await this.connection.write(this.stateValueToPoint(pointId, values));
      this.log.debug(`Point written to ${this.database}`);
      callback_error();
    } catch (error) {
      this.log.warn(`Point could not be written to database: ${this.database}`);
      callback_error(error);
    }
  }

  stateValueToPoint(pointName, stateValue) {
    let point = null;

    point = InfluxClient.Point.measurement(pointName);
    point.setTimestamp(stateValue.time);
    if (this.useTags) {
      point.setTag("q", String(stateValue.q));
      point.setTag("ack", String(stateValue.ack));
      point.setTag("from", stateValue.from);
    } else {
      point = new InfluxClient.Point(pointName);
      point.setFloatField("q", stateValue.q);
      point.setBooleanField("ack", stateValue.ack);
      point.setStringField("from", stateValue.from);
    }

    switch (typeof stateValue.value) {
      case "boolean":
        point.setBooleanField("value", stateValue.value);
        break;
      case "number":
        point.setFloatField("value", parseFloat(stateValue.value));
        break;
      case "string":
      default:
        point.setStringField("value", stateValue.value);
        break;
    }
    return point;
  }

  async query(query, callback) {
    this.log.debug(`Query to execute: ${query}`);
    const queries = query.replace(/;\W*$/, "").split(";");
    if (queries.length > 1) {
      this.queries(queries, callback);
    } else {
      try {
        const queryResult = await this.connection.query(query, this.database,{ type: 'influxql'});
        const rows = [];
        for await (const row of queryResult) {
            rows.push(row)
        }
        callback(null, rows);
      } catch (error) {
        callback(error, null);
      }
    }
  }

  /*ping(interval) {
        // Ping is only used internally. As _pool is private it won't work like below
        this.connection._pool.ping(interval);
    }*/
}

module.exports = {
  DatabaseInfluxDB3x: DatabaseInfluxDB3x,
};
