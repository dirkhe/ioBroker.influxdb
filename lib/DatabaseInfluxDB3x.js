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
    requestTimeout,
    validateSSL,
    useTags,
    timePrecision
  ) {
    super();

    this.log = log;

    this.host = host;
    this.port = port || 8181;
    this.protocol = protocol;
    this.path = path;
    this.token = token;
    this.database = database;
    this.timePrecision = timePrecision; // ms
    this.requestTimeout = requestTimeout; // 30000
    this.useTags = useTags || false;
    this.validateSSL = validateSSL;

    this.request = new Database.FakeConnectionPool();
    //disable ping
    this.ping = false;

    this.connect();
  }

  connect() {
    const url = `${this.protocol}://${this.host}:${this.port}`;
    this.log.debug(`Connect InfluxDB3: ${url} [${this.database}]`);

    const options = {
      host: url,
      token: this.token,
      database: this.database,
      timeout: this.requestTimeout,
      transportOptions: { rejectUnauthorized: this.validateSSL },
      proxyUrl: this.path,
    };
    if (this.timePrecision) {
      if (!options.writeOptions) options.writeOptions = {};
      options.writeOptions.precision = this.timePrecision;
    }

    this.connection = new InfluxClient.InfluxDBClient(options);

    this.request.activatePool();
  }

  async getDatabaseNames(callback) {
    try {
      this.query("SHOW FIELD KEYS", (err, rows) => {
        if (err) {
          callback(err, null);
        } else {
          this.DBisEmpty = rows.length === 0;
          const foundDatabases = [];
          foundDatabases.push(this.database);
          callback(null, foundDatabases);
        }
      });
    } catch (error) {
      callback(error, null);
    }
  }

  getMetaDataStorageType(callback) {
    this.query("SHOW TAG KEYS", (error, rows) => {
      let storageType = "none";
      if (error) {
        callback(error, null);
      } else {
        if (rows.length > 0){
            storageType = "tags" 
        } else if (!this.DBisEmpty) {
            storageType = "fields";
        }
        callback(null, storageType);
      }
    });
  }

  getRetentionPolicyForDB(dbname, callback) {
    return callback("RetentionPolicy not available for influxDB3", null); // only needed for InfluxQL
  }

  async applyRetentionPolicyToDB(dbname, retention, callback) {
    return callback("RetentionPolicy could noct change for influxDB3, you have to do this manually");
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
        const queryResult = await this.connection.query(query, this.database, {
          type: "influxql",
        });
        const rows = [];
        for await (const row of queryResult) {
          rows.push(row);
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
