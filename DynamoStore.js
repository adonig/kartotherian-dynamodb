'use strict';

/*

 DynamoStore is a DynamoDB tile storage.
 */

var util = require('util');
var Promise = require('bluebird');
var AWS = require('aws-sdk');
var multistream = require('multistream');
var promistreamus = require('promistreamus');
var pckg = require('./package.json');

var core, Err;

function DynamoStore(uri, callback) {
    var self = this;
    this.batchMode = 0;
    this.batch = [];

    this.throwError = function (msg) {
        throw new Error(util.format.apply(null, arguments) + JSON.stringify(uri));
    };

    this.attachUri = function (err) {
        err.moduleUri = JSON.stringify(self._params);
        throw err;
    };

    this.parseWithDefault = function(param, defaultValue, parsefn) {
        parsefn = parsefn || function(x) { return x; }; // identity if undefined
        return (typeof param === 'undefined') ? defaultValue : parsefn(param);
    };

    return Promise.try(function () {
        self.headers = {
            'Content-Type': 'application/x-protobuf',
            'Content-Encoding': 'gzip'
        };
        var params = core.normalizeUri(uri).query;
        self._params = params;

        if (!params.region || !/^[a-zA-Z0-9\-]*$/.test(params.region)) {
            self.throwError("Uri must have a valid 'region' query parameter");
        }
        if (params.table && !/^[a-zA-Z0-9\_\-\.]*$/.test(params.table)) {
            self.throwError("Optional uri 'table' param must be a valid value");
        }
        self.region = params.region;
        self.createIfMissing = !!params.createIfMissing;
        self.table = params.table || 'Tiles';
        self.readCapacityUnits = self.parseWithDefault(params.readCapacityUnits, 5, parseInt);
        self.writeCapacityUnits = self.parseWithDefault(params.writeCapacityUnits, 5, parseInt);
        self.minzoom = self.parseWithDefault(params.minzoom, 0, parseInt);
        self.maxzoom = self.parseWithDefault(params.maxzoom, 22, parseInt);
        self.maxBatchSize = self.parseWithDefault(params.maxBatchSize, undefined, parseInt);
        self.client = new AWS.DynamoDB({region: params.region});
        return true;
    }).then(function () {
        if (!self.createIfMissing) {
            return true;
        }
        return self.client.describeTable({
            TableName: self.table
        }).promise();
    }).catch(function (err) {
        var params = {
            TableName: self.table,
            KeySchema: [
                { AttributeName: "zoom", KeyType: "HASH"},
                { AttributeName: "idx", KeyType: "RANGE"}
            ],
            AttributeDefinitions: [
                { AttributeName: "zoom", AttributeType: "N" },
                { AttributeName: "idx", AttributeType: "N" }
            ],
            ProvisionedThroughput: {
                ReadCapacityUnits: self.readCapacityUnits, 
                WriteCapacityUnits: self.writeCapacityUnits
            }
        };
        return self.client.createTable(params).promise();
    }).catch(function (err) {
        return self.closeAsync().finally(function () {
            throw err;
        });
    }).then(function () {
        return self;
    }).catch(this.attachUri).nodeify(callback);
}

DynamoStore.prototype.getTile = function(z, x, y, callback) {
    var self = this;
    return Promise.try(function () {
        if (z < self.minzoom || z > self.maxzoom) {
            core.throwNoTile();
        }
        return self.queryTileAsync({
            "zoom": z,
            "idx": core.xyToIndex(x, y, z)
        });
    }).then(function (row) {
        if (!row) {
            core.throwNoTile();
        }
        return [row.tile, self.headers];
    }).nodeify(callback, {spread: true});
};

DynamoStore.prototype.putInfo = function(data, callback) {
    // hack: Store source info under zoom -1 with ID 0
    return this._storeDataAsync(-1, 0, new Buffer(JSON.stringify(data))).nodeify(callback);
};

DynamoStore.prototype.getInfo = function(callback) {
    var self = this;
    return this.queryTileAsync({info: true}).then(function (row) {
        if (row) {
            return JSON.parse(row.tile.toString());
        } else {
            return {
                'tilejson': '2.1.0',
                'name': 'DynamoStore ' + pckg.version,
                'bounds': '-180,-85.0511,180,85.0511',
                'minzoom': self.minzoom,
                'maxzoom': self.maxzoom
            };
        }
    }).catch(this.attachUri).nodeify(callback);
};

DynamoStore.prototype.putTile = function(z, x, y, tile, callback) {
    if (z < this.minzoom || z > this.maxzoom) {
        throw new Err('This DynamoStore source cannot save zoom %d, because its configured for zooms %d..%d',
            z, this.minzoom, this.maxzoom);
    }
    return this._storeDataAsync(z, core.xyToIndex(x, y, z), tile).nodeify(callback);
};

DynamoStore.prototype._storeDataAsync = function(zoom, idx, data) {
    var self = this;
    return Promise.try(function () {
        var params, action, batchRequest;
        if (data && data.length > 0) {
            params = {
                TableName: self.table,
                Item: {
                    "zoom": {"N": zoom.toString()},
                    "idx": {"N": idx.toString()},
                    "tile": {"B": data}
                }
            };
            action = "Put";
        } else {
            params = {
                TableName: self.table,
                Key: {
                    "zoom": {"N": zoom.toString()},
                    "idx": {"N": idx.toString()}
                }
            };
            action = "Delete";
        }
        if (!self.batchMode || !self.maxBatchSize) {
            var functionName = action.toLowerCase() + 'Item';
            return self.client[functionName](params).promise();
        } else {
            var requestName = action + 'Request';
            self.batch.push({requestName: params});
            if (Object.keys(self.batch).length > self.maxBatchSize) {
                return self.flushAsync();
            }
        }
    }).catch(this.attachUri);
};

DynamoStore.prototype.close = function(callback) {
    if (!this.client) {
        callback(null);
    } else {
        var self = this;
        Promise.try(function () {
            return (self.batchMode && self.maxBatchSize) ? self.flushAsync() : true;
        }).then(function () {
            delete self.client;
            self.batchMode = 0;
            return true;
        }).catch(this.attachUri).nodeify(callback);
    }
};

DynamoStore.prototype.startWriting = function(callback) {
    this.batchMode++;
    callback(null);
};

DynamoStore.prototype.flush = function(callback) {
    var batch = this.batch;
    if (Object.keys(batch).length > 0) {
        this.batch = [];
        var params = {RequestItems: {}};
        params.RequestItems[self.table] = batch;
        this.client.batchWriteItem(params).promise().catch(this.attachUri).nodeify(callback);
    } else {
        callback();
    }
};

DynamoStore.prototype.stopWriting = function(callback) {
    var self = this;
    Promise.try(function () {
        if (self.batchMode === 0) {
            self.throwError('stopWriting() called more times than startWriting()');
        }
        self.batchMode--;
        return self.flushAsync();
    }).catch(this.attachUri).nodeify(callback);
};

/**
 * Iterate all tiles that match the given parameters
 * @param {object} options
 * @param {number} options.idx Index of the tile
 * @param {number} options.zoom
 * @param {boolean} options.getSize
 * @param {boolean} options.getTile
 * @param {boolean} options.getWriteTime
 * @param {boolean} options.info
 * @returns {*}
 */
DynamoStore.prototype.queryTileAsync = function(options) {
    var self = this, getTile, getWriteTime, getSize;

    return Promise.try(function() {
        if (options.info) {
            options.zoom = -1;
            options.idx = 0;
        } else {
            if (!core.isInteger(options.zoom))
                self.throwError('Options must contain integer zoom parameter. Opts=%j', options);
            if (!core.isInteger(options.idx))
                self.throwError('Options must contain an integer idx parameter. Opts=%j', options);
            var maxEnd = Math.pow(4, options.zoom);
            if (options.idx < 0 || options.idx >= maxEnd)
                self.throwError('Options must satisfy: 0 <= idx < %d. Opts=%j', maxEnd, options);
        }
        if (options.getWriteTime)
            self.throwError('getWriteTime is not implemented by Dynamo source. Opts=%j', options);
        getTile = typeof options.getTile === 'undefined' ? true : options.getTile;
        getSize = typeof options.getSize === 'undefined' ? false : options.getSize;
        return self.client.getItem({
            TableName : self.table,
            ProjectionExpression: "tile",
            Key: {
                "zoom": {"N": options.zoom.toString()},
                "idx": {"N": options.idx.toString()}
            }
        }).promise();
    }).then(function(res) {
        if ('Items' in res && res.Items.length === 1) {
            var item = res.Items[0];
            var resp = {};
            if (getTile) resp.tile = item.tile;
            if (getSize) resp.size = item.tile.length;
            return resp;
        } else {
            return false;
        }
    }).catch(this.attachUri);
};

/**
 * Query database for all tiles that match conditions
 * @param options - an object that must have an integer 'zoom' value.
 * Optional values:
 *  idxFrom - int index to start iteration from (inclusive)
 *  idxBefore - int index to stop iteration at (exclusive)
 *  dateFrom - Date value - return only tiles whose write time is on or after this date (inclusive)
 *  dateBefore - Date value - return only tiles whose write time is before this date (exclusive)
 *  biggerThan - number - return only tiles whose compressed size is bigger than this value (inclusive)
 *  smallerThan - number - return only tiles whose compressed size is smaller than this value (exclusive)
 * @returns {Function} - a function that returns a promise. If promise resolves to undefined, there are no more values
 * in the stream. The promised values will contain:
 *  {number} zoom
 *  {number} idx
 *  {Buffer} tile if options.getTiles is set, get tile data
 *  {object} headers if options.getTiles is set, get tile header
 */
DynamoStore.prototype.query = function(options) {
    var self = this,
        dateBefore, dateFrom;

    if (!core.isInteger(options.zoom))
        self.throwError('Options must contain integer zoom parameter. Opts=%j', options);
    if (typeof options.idxFrom !== 'undefined' && !core.isInteger(options.idxFrom))
        self.throwError('Options may contain an integer idxFrom parameter. Opts=%j', options);
    if (typeof options.idxBefore !== 'undefined' && !core.isInteger(options.idxBefore))
        self.throwError('Options may contain an integer idxBefore parameter. Opts=%j', options);
    if (typeof options.dateBefore !== 'undefined' && Object.prototype.toString.call(options.dateBefore) !== '[object Date]')
        self.throwError('Options may contain a Date dateBefore parameter. Opts=%j', options);
    if (typeof options.dateFrom !== 'undefined' && Object.prototype.toString.call(options.dateFrom) !== '[object Date]')
        self.throwError('Options may contain a Date dateFrom parameter. Opts=%j', options);
    if (typeof options.biggerThan !== 'undefined' && typeof options.biggerThan !== 'number')
        self.throwError('Options may contain a biggerThan numeric parameter. Opts=%j', options);
    if ((typeof options.smallerThan !== 'undefined' && typeof options.smallerThan !== 'number') || options.smallerThan <= 0)
        self.throwError('Options may contain a smallerThan numeric parameter that is bigger than 0. Opts=%j', options);
    var maxEnd = Math.pow(4, options.zoom);
    var start = options.idxFrom || 0;
    var end = options.idxBefore || maxEnd;
    if (start > end || end > maxEnd)
        self.throwError('Options must satisfy: idxFrom <= idxBefore <= %d. Opts=%j', maxEnd, options);
    if (options.dateFrom >= options.dateBefore)
        self.throwError('Options must satisfy: dateFrom < dateBefore. Opts=%j', options);
    dateFrom = options.dateFrom ? options.dateFrom.valueOf() * 1000 : false;
    dateBefore = options.dateBefore ? options.dateBefore.valueOf() * 1000 : false;

    var fields = 'idx';
    if (options.getTiles || options.smallerThan || options.biggerThan) {
        // If tile size check is requested, we have to get the whole tile at this point...
        // Optimization - if biggerThan is 0, it will not be used
        fields += ', tile';
    }
    if (dateBefore !== false || dateFrom !== false) {
        self.throwError('date filtering is not implemented for Dynamo store');
    }


    var createStream = function(blockStart, blockEnd) {
        var params = {
            TableName: self.table,
            ProjectionExpression: fields,
            KeyConditionExpression: "zoom = :zoom",
            ExpressionAttributeValues: {
                ":zoom": options.zoom
            }
        };
        // options.zoom===0 is a temp workaround because the info data used
        // to be stored in the zoom=0,idx=1
        if (start > blockStart && (end < blockEnd || options.zoom === 0)) {
            params.KeyConditionExpression += " AND idx BETWEEN :start AND :end";
            params.ExpressionAttributeValues[":start"] = start;
            params.ExpressionAttributeValues[":end"] = end;
        } else if (start > blockStart) {
            params.KeyConditionExpression += " AND idx >= :start";
            params.ExpressionAttributeValues[":start"] = start;
        } else if (end < blockEnd || options.zoom === 0) {
            params.KeyConditionExpression += " AND idx < :end";
            params.ExpressionAttributeValues[":end"] = end;
        }
        return self.client.query(params);
    };

    var ms = createStream(0, maxEnd);

    return promistreamus(ms, function(value) {
        if ((dateBefore !== false && value.wt >= dateBefore) ||
            (dateFrom !== false && value.wt < dateFrom) ||
            (options.smallerThan && value.tile.length >= options.smallerThan) ||
            (options.biggerThan && value.tile.length < options.biggerThan)
        ) {
            return undefined;
        }
        var res = {
            "zoom": options.zoom,
            "idx": (typeof value.idx === 'number' ? value.idx : value.idx.toNumber())
        };
        if (options.getTiles) {
            res.tile = value.tile;
            res.headers = self.headers;
        }
        return res;
    });
};


DynamoStore.initKartotherian = function(cor) {
    core = cor;
    Err = core.Err;
    core.tilelive.protocols['dynamodb:'] = DynamoStore;
};

Promise.promisifyAll(DynamoStore.prototype);
module.exports = DynamoStore;
