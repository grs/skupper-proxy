/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

var fs = require('fs');
var path = require('path');
var util = require('util');

var log = require('./log.js').logger();

var read_file = util.promisify(fs.readFile);

function plural(input) {
    return input + 's';
}

function read_router_config (conf_file) {
    return read_file(conf_file).then(function (data) {
        var json = JSON.parse(data);
        var router_config = {};
        for (var i in json) {
            var type = json[i][0]
            var record = json[i][1];
            if (type === 'router' || type === 'policy') {
                // singletons with global settings
                router_config[type] = record;
            } else {
                var list = router_config[plural(type)];
                if (list === undefined) {
                    list = [];
                    router_config[plural(type)] = list;
                }
                list.push(record);
            }
        }
        return router_config;
    });
};

function get_filtered_callback(name, callback) {
    return function (event, filename) {
        if (filename === name) {
            callback(event, filename);
        } else {
            log.info('ignoring watch event for %s (does not match %s)', filename, name);
        }
    };
}

function watch(filename, callback) {
    fs.lstat(filename, function (e, stats) {
        if (e) {
            log.error(e);
        } else if (stats.isSymbolicLink()) {
            fs.readlink (filename, function (e, actual) {
                if (e) {
                    log.error(e);
                } else {
                    fs.watch(path.dirname(filename), get_filtered_callback(path.basename(path.dirname(actual)), callback));
                }
            });
        } else {
            fs.watch(filename, callback);
        }
    });
}

module.exports.read_router_config = read_router_config;

module.exports.watch_router_config = function (conf_file, callback) {
    watch(conf_file, function (event, filename) {
        log.info('%s %s', filename, event)
        read_router_config(conf_file).then(function (config) {
            callback(undefined, config);
        }).catch(function (error) {
            callback(error, undefined);
        });
    });
};
