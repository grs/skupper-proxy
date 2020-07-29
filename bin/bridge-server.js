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

var amqp_mgmt = require('../lib/amqp_mgmt.js');
var bridges = require('../lib/bridges.js');
var eventchannel = require('../lib/eventchannel.js');
var multicast = require('../lib/multicast.js');
var metrics = require('../lib/metrics.js');
var qdrconf = require('../lib/qdrconf.js');
var myutils = require('../lib/utils.js');
var log = require('../lib/log.js').logger();

function create_http_connector (attributes) {
    var bridge;
    if (attributes.aggregate) {
        bridge = multicast.amqp_to_http(attributes.address, attributes.host, attributes.port);
    } else if (attributes.eventchannel) {
        bridge = eventchannel.amqp_to_http(attributes.address, attributes.host, attributes.port);
    } else {
        bridge = bridges.amqp_to_http(attributes.address, attributes.host, attributes.port);
    }
    bridge.metrics = metrics.create_bridge_metrics(attributes.address, 'http', attributes.siteId);
    bridge.siteId = attributes.siteId;
    bridge.site_id = attributes.siteId;
    if (attributes.hostOverride) {
        bridge.hostOverride = attributes.hostOverride
    }
    return bridge;
}

function delete_http_connector (object) {
    object.stop();
}

function describe_http_connector (object) {
    return {
        host: object.host,
        port: object.port,
        address: object.address,
        siteId: object.siteId,
        hostOverride: object.hostOverride
    };
}


function create_http_listener (attributes) {
    var bridge;
    if (attributes.aggregate) {
        bridge = multicast.http_to_amqp(attributes.port, attributes.address, config.aggregate);
    } else if (attributes.eventchannel) {
        bridge = eventchannel.http_to_amqp(attributes.port, attributes.address);
    } else {
        bridge = bridges.http_to_amqp(attributes.port, attributes.address);
    }
    bridge.metrics = metrics.create_bridge_metrics(attributes.address, 'http', attributes.siteId);
    bridge.siteId = attributes.siteId;
    bridge.site_id = attributes.siteId;
    return bridge;
}

function delete_http_listener (object) {
    object.stop();
}

function describe_http_listener (object) {
    return {
        port: object.port,
        address: object.address,
        siteId: object.siteId,
    };
}

function create_http2_connector (attributes) {
    var bridge = bridges.amqp_to_http2(attributes.address, attributes.host, attributes.port);
    bridge.metrics = metrics.create_bridge_metrics(attributes.address, 'http2', attributes.siteId);
    bridge.siteId = attributes.siteId;
    bridge.site_id = attributes.siteId;
    return bridge;
}

function delete_http2_connector (object) {
    object.stop();
}

function describe_http2_connector (object) {
    return {
        host: object.host,
        port: object.port,
        address: object.address,
        siteId: object.siteId,
    };
}


function create_http2_listener (attributes) {
    var bridge = bridges.http2_to_amqp(attributes.port, attributes.address);
    bridge.metrics = metrics.create_bridge_metrics(attributes.address, 'http2', attributes.siteId);
    bridge.siteId = attributes.siteId;
    bridge.site_id = attributes.siteId;
    return bridge;
}

function delete_http2_listener (object) {
    object.stop();
}

function describe_http2_listener (object) {
    return {
        port: object.port,
        address: object.address,
        siteId: object.siteId,
    };
}

function create_tcp_connector (attributes) {
    var bridge = bridges.amqp_to_tcp(attributes.address, attributes.host, attributes.port);
    bridge.metrics = metrics.create_bridge_metrics(attributes.address, 'tcp', attributes.siteId);
    bridge.siteId = attributes.siteId;
    bridge.site_id = attributes.siteId;
    return bridge;
}

function delete_tcp_connector (object) {
    object.stop();
}

function describe_tcp_connector (object) {
    return {
        host: object.host,
        port: object.port,
        address: object.address,
        siteId: object.siteId,
    };
}

function create_tcp_listener (attributes) {
    var bridge = bridges.tcp_to_amqp(attributes.port, attributes.address);
    bridge.metrics = metrics.create_bridge_metrics(attributes.address, 'tcp', attributes.siteId);
    bridge.siteId = attributes.siteId;
    bridge.site_id = attributes.siteId;
    return bridge;
}

function delete_tcp_listener (object) {
    object.stop();
}

function describe_tcp_listener (object) {
    return {
        port: object.port,
        address: object.address,
        siteId: object.siteId,
    };
}

function equivalent(a, b) {
    for (var f in a) {
        if (b[f] && a[f] !== b[f]) {
            log.info('%j and %j are not equivalent: %s !== %s', a, b, a[f], b[f]);
            return false;
        }
    }
    return true;
}

const type_prefix = 'org.apache.qpid.dispatch.router.';

function EntityManager (typename, configname, constructor, destructor, describer) {
    this.typename = type_prefix + typename;
    this.configname = configname;
    this.objects = {};
    this.constructor = constructor;
    this.destructor = destructor;
    this.describer = describer;
}

EntityManager.prototype.query = function () {
    var result = [];
    for (var name in this.objects) {
        result.push(myutils.merge(this.describer(this.objects[name]), {name:name, identity:name, type:this.typename, metrics: this.objects[name].metrics}));
    }
    return result;
};

EntityManager.prototype.create = function (name, attributes) {
    if (this.objects[name]) {
        throw new Error(this.type + ' with name ' + name + ' already exists')
    }
    log.info('adding %s named %s', this.typename, name);
    var object = this.constructor(attributes);
    this.objects[name] = object;
    return this.describer(object);
};

EntityManager.prototype.delete = function (name) {
    log.info('deleting %s named %s', this.typename, name);
    var object = this.objects[name];
    delete this.objects[name];
    if (object) {
        this.destructor(object);
    }
};

EntityManager.prototype.update = function (desired) {
    log.info('checking for %s updates...', this.typename);
    for (var name in this.objects) {
        if (desired[name] === undefined || !equivalent(this.describer(this.objects[name]), desired[name])) {
            this.delete(name);
        }
    }
    for (var name in desired) {
        if (this.objects[name] === undefined) {
            this.create(name, desired[name]);
        }
    }
};

function index_by_name(a, b) {
    a[b.name] = b;
    return a;
}

function Server(config_file) {
    var http_connector = new EntityManager('httpConnector', 'httpConnectors', create_http_connector, delete_http_connector, describe_http_connector);
    var http_listener = new EntityManager('httpListener', 'httpListeners', create_http_listener, delete_http_listener, describe_http_listener);
    var http2_connector = new EntityManager('http2Connector', 'http2Connectors', create_http2_connector, delete_http2_connector, describe_http2_connector);
    var http2_listener = new EntityManager('http2Listener', 'http2Listeners', create_http2_listener, delete_http2_listener, describe_http2_listener);
    var tcp_connector = new EntityManager('tcpConnector', 'tcpConnectors', create_tcp_connector, delete_tcp_connector, describe_tcp_connector);
    var tcp_listener = new EntityManager('tcpListener', 'tcpListeners', create_tcp_listener, delete_tcp_listener, describe_tcp_listener);

    this.typedefs = [http_connector, http_listener, http2_connector, http2_listener, tcp_connector, tcp_listener];
    this.management_server = amqp_mgmt.server();
    var self = this;
    this.typedefs.forEach(function (typedef) {
        self.management_server.register(typedef.typename, typedef)
    });
    qdrconf.read_router_config(config_file).then(function (config) {
        log.info('config read: %j', config);
        self.config_updated(config);
        //self.management_server.listen(5677); //todo: make the mode configurable?
        self.management_server.connect();
        qdrconf.watch_router_config(config_file, function (error, config) {
            if (error) {
                log.error('Failed to get config update: %s', error);
            } else {
                log.info('config updated: %j', config);
                self.config_updated(config);
            }
        });
    });
}

Server.prototype.config_updated = function (config) {
    this.typedefs.forEach(function (typedef) {
        var entities = config[typedef.configname];
        if (entities) {
            log.info('%s defined: %j', typedef.configname, entities);
            typedef.update(entities.reduce(index_by_name, {}));
        } else {
            log.info('no %s defined', typedef.configname);
            typedef.update({});
        }
    });
};

process.on('SIGTERM', function () {
    console.log('Exiting due to SIGTERM');
    process.exit();
});

var server = new Server(process.argv[2] || process.env.CONF_FILE || '/etc/qpid-dispatch/qdrouter.js');
