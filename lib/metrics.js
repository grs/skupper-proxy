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

var http = require('http');
var rhea = require('rhea');
var kubernetes = require('./kubernetes.js').client();
var log = require('./log.js').logger();
var qdr = require('./qdr.js');
var myutils = require('./utils.js');

function http_request_metrics() {
    return {
        requests: 0,
        bytes_in: 0,
        bytes_out: 0,
        details: {},//request count keyed by method+response_code
        latency_max: 0//TODO: add percentiles
    };
}

function record_http_request (metrics, method, response_code, bytes_in, bytes_out, latency) {
    metrics.requests++;
    metrics.bytes_in += bytes_in;
    metrics.bytes_out += bytes_out;
    if (latency > metrics.latency_max) {
        metrics.latency_max = latency;
    }
    var key = method + ':' + response_code;
    if (metrics.details[key] === undefined) {
        metrics.details[key] = 1;
    } else {
        metrics.details[key]++;
    }
};

function record_indexed_http_request (index, key, method, response_code, bytes_in, bytes_out, latency) {
    var metrics = index[key];
    if (metrics === undefined) {
        metrics = http_request_metrics();
        index[key] = metrics;
    }
    record_http_request(metrics, method, response_code, bytes_in, bytes_out, latency);
}

function merge_counts(a, b) {
    for (var k in b) {
        if (a[k]) {
            a[k] += b[k];
        } else {
            a[k] = b[k];
        }
    }
    return a;
}

function merge_http_request_metrics(a, b) {
    a.requests += b.requests;
    a.bytes_in += b.bytes_in;
    a.bytes_out += b.bytes_out;
    merge_counts(a.details, b.details);
    //TODO: add support for percentiles
    if (b.latency_max > a.latency_max) {
        a.latency_max = b.latency_max;
    }
    return a;
}

function merged_indexed_http_request_metrics (a, b) {
    if (a === undefined) return b;
    if (b === undefined) return a;
    for (var k in b) {
        if (a[k]) {
            merge_http_request_metrics(a[k], b[k]);
        } else {
            a[k] = b[k];
        }
    }
    return a;
}

function http_ingress() {
    var metrics = http_request_metrics();
    metrics.by_client = {};
    return metrics;
}

function http_egress() {
    var metrics = http_request_metrics();
    metrics.by_server = {};
    return metrics;
}

function http_metrics (address) {
    return {
        address: address,
        protocol: 'http',
        ingress: http_ingress(),
        egress: http_egress()
    };
}

function BridgeMetricsAgent (origin, metrics) {
    this.origin = origin
    this.container = rhea.create_container({id:'bridge-metrics-' + origin, enable_sasl_external:true})
    this.connection = this.container.connect();
    this.connection.open_receiver("mc/$"+origin);
    this.sender = this.connection.open_sender({target:{}});
    this.connection.on('message', this.on_message.bind(this));
    this.metrics = metrics;
}

BridgeMetricsAgent.prototype.on_message = function (context) {
    log.info('bridge metrics agent received %j', context.message);
    if (context.message.subject === 'bridge-metrics-request') {
        this.sender.send({to: context.message.reply_to, correlation_id:context.message.correlation_id, subject: 'bridge-metrics', group_id: process.env.HOSTNAME, body: JSON.stringify(this.metrics)});
        log.info('bridge metrics agent sent reply');
    }
}

function translate_keys(map, lookup) {
    var results = {};
    for (var k in map) {
        var k2 = lookup[k];
        if (k2) {
            results[k2] = map[k];
        } else {
            results[k] = map[k];
        }
    }
    log.info('translate_keys(%j, %j) => %j', map, lookup, results);
    return results;
}

function set(entries) {
    var items = {};
    var count = 0;
    entries.forEach(function (e) {
        items[e] = e;
        count++;
    });
    return {
        add: function (item) {
            if (items[item] !== undefined) {
                items[item] = item;
                count++;
                return true;
            } else {
                return false;
            }
        },
        remove: function (item) {
            if (items[item] !== undefined) {
                delete items[item];
                count--;
                return true;
            } else {
                return false;
            }
        },
        empty: function () {
            return count === 0;
        },
        has: function (item) {
            return items[item] !== undefined;
        },
        items: function () {
            return Object.keys(items);
        }
    };
}

function AggregatedBridgeMetrics(proxies, ip_lookup, resolve, reject) {
    this.expected = set(proxies);
    this.ip_lookup = ip_lookup;
    this.resolve = resolve;
    this.reject = reject;
    this.metrics = {};
}

AggregatedBridgeMetrics.prototype.is_complete = function () {
    return this.expected.empty();
};

AggregatedBridgeMetrics.prototype.add_metrics = function (source, added) {
    log.info('handling bridge metrics from %s: %j', source, added);
    if (this.expected.remove(source)) {
        var by_client = translate_keys(added.ingress.by_client, this.ip_lookup);
        var by_server = translate_keys(added.egress.by_server, this.ip_lookup);
        var current = this.metrics[added.address];
        if (current === undefined) {
            this.metrics[added.address] = {
                protocol: added.protocol,
                requests_received: {
                    requests: added.ingress.requests,
                    bytes_in: added.ingress.bytes_in,
                    bytes_out: added.ingress.bytes_out,
                    details: added.ingress.details
                },
                requests_handled: {
                    requests: added.egress.requests,
                    bytes_in: added.egress.bytes_in,
                    bytes_out: added.egress.bytes_out,
                    details: added.egress.details
                },
                by_client: by_client,
                by_server: by_server
            };
        } else {
            // have more than one proxy pod in local site serving the
            // same address
            current.requests_received.requests += added.ingress.requests;
            current.requests_received.bytes_in += added.ingress.bytes_in;
            current.requests_received.bytes_out += added.ingress.bytes_out;
            merge_counts(current.requests_received.details, added.ingress.details);

            current.requests_handled.requests += added.egress.requests;
            current.requests_handled.bytes_in += added.egress.bytes_in;
            current.requests_handled.bytes_out += added.egress.bytes_out;
            merge_counts(current.requests_handled.details, added.egress.details);
            merged_indexed_http_request_metrics(current.requests_handled.by_client, by_client);
            merged_indexed_http_request_metrics(current.requests_handled.by_server, by_server);
        }

        if (this.expected.empty()) {
            log.info('have received all expected bridge-metrics');
            this.resolve(this.metrics);
        } else {
            log.info('still waiting for bridge-metrics from %j', this.expected.items());
        }
    } else {
        log.info('duplicate bridge metrics from %s', source);
    }
};

function get_site_name(name) {
    var end = name.indexOf('-skupper-router');
    return end > 0 ? name.substring(0, end) : name;
}

function get_values(map) {
    var result = [];
    for (var key in map) {
        result.push(map[key]);
    }
    return result;
}

function AggregatedSiteMetrics(sites, resolve, reject) {
    this.expected = set(sites);
    this.resolve = resolve;
    this.reject = reject;
    this.sites = [];
    this.services = {};
    this.metrics = {};
}

AggregatedSiteMetrics.prototype.is_complete = function () {
    return this.expected.empty();
};

AggregatedSiteMetrics.prototype.add_metrics = function (source, added) {
    if (this.expected.remove(source)) {
        //aggregate the results
        //site_name, site_id, bridge_metrics (which is keyed on address)
        //want to combine by address

        //sites
        this.sites.push({
            site_name: get_site_name(added.site_name),
            site_id: added.site_id,
            connected: added.connected_sites
            //TODO: add namespace and exposed urls
        });
        //services
        for (var address in added.bridge_metrics) {
            var service = this.services[address];
            var _service = added.bridge_metrics[address];
            if (service === undefined) {
                service = {
                    address: address,
                    protocol: _service.protocol,
                    requests_received: [],
                    requests_handled: [],
                    targets: []
                };
                this.services[address] = service;
            } else {
                if (service.protocol != _service.protocol) {
                    log.error('protocol mismatch for %s, have %s but site %s has %s', address, service.protocol, added.site_id, _service.protocol);
                }
            }
            var received = _service.requests_received;
            received.site_id = added.site_id;
            received.by_client = _service.by_client;
            service.requests_received.push(received);

            var handled = _service.requests_handled;
            handled.site_id = added.site_id;
            handled.by_server = _service.by_server;
            service.requests_handled.push(handled);

            var targets = added.targets[address];
            if (targets) {
                targets.forEach(function (target) {
                    service.targets.push({
                        name: target.name,
                        target: target.target,
                        site_id: added.site_id
                    });
                });
            } else {
                log.info('No target for %s in %j', address, added.targets);
            }
        }

        //this.metrics[source] = added;//TODO: FIX-ME!!

        if (this.expected.empty()) {
            //this.resolve(this.metrics);
            this.resolve({
                sites: this.sites,
                services: get_values(this.services)
            });
        } else {
            log.info('still waiting for site-metrics from %j', this.expected.items());
        }
    } else {
        log.info('duplicate site metrics from %s', source);
    }
};

function pod_ready (pod) {
    for (var i in pod.status.conditions) {
        if (pod.status.conditions[i].type === 'Ready') {
            return pod.status.conditions[i].status === 'True';
        }
    }
    return false;
}

function pod_running (pod) {
    return pod.status.phase === 'Running';
}

function pod_ready_and_running (pod) {
    return pod_ready(pod) && pod_running(pod);
}

function is_proxy_pod (pod) {
    //TODO: change to use internal.skupper.io/type === proxy
    return pod.metadata.labels && pod.metadata.labels['internal.skupper.io/service'] != undefined;
}

function pod_summary (pod) {
    return {
        name: pod.metadata.name,
        ip: pod.status.podIP,
        labels: pod.metadata.labels
    };
}

function get_pod_name (pod) {
    return pod.metadata.name;
}

function equivalent_pod (a, b) {
    return a.name === b.name && a.ip === b.ip;
}

function name_compare (a, b) {
    return myutils.string_compare(a.name, b.name);
}

function parse_selector (selector) {
    var elements = selector.split(',');
    var labels = {};
    for (var i in elements) {
        var parts = elements[i].split('=');
        labels[parts[0]] = parts[1] || null;
    }
    return function (podsummary) {
        for (var key in labels) {
            if (!(podsummary.labels[key] && (labels[key] === null || labels[key] === podsummary.labels[key]))) {
                log.debug('pod with labels %j does not match selector %j on %s', podsummary.labels, labels, key);
                return false;
            }
        }
        return true;
    };
}

function get_matcher (service) {
    var matchers = service.targets ? service.targets.map(function (target) {
        return {
            name: target.name,
            match: parse_selector(target.selector)
        }
    }) : [];
    return function (pod, matched) {
        for (var i in matchers) {
            if (matchers[i].match(pod)) {
                matched.push({
                    name: pod.name,
                    target: matchers[i].name
                });
            }
        }
    };
}

function SiteMetricsAgent(origin) {
    this.config_watcher = kubernetes.watch_resource('configmaps', 'skupper-services');
    this.config_watcher.on('updated', this.service_definitions_updated.bind(this));
    this.service_mapping = undefined; // pod name -> list of services (and target names) of which that pod is a target

    this.pod_watcher = kubernetes.watch('pods');
    this.pod_watcher.on('updated', this.pods_updated.bind(this));
    this.ip_lookup = {}; // ip -> name
    this.current_pods = []; // summary of all pods in namespace, used to maintain ip_lookup and service_mapping
    this.proxies = []; // list of the proxy pods (used to determine when all bridge results have been received)

    this.origin = origin;
    this.container = rhea.create_container({id:'metrics-collector' + origin, enable_sasl_external:true});
    this.connection = this.container.connect();
    this.connection.open_receiver({source:{dynamic:true}}).on('receiver_open', this.on_receiver_open.bind(this));
    this.local_sender = this.connection.open_sender("mc/$"+origin);
    this.connection.open_receiver("mc/$controllers");
    this.global_sender = this.connection.open_sender("mc/$controllers");
    this.connection.on('message', this.on_message.bind(this));
    this.address = undefined;
    this.pending = [];

    this.ready = [];
    this.counter = 1;
    this.inprogress_bridge_metrics_requests = {};
    this.inprogress_site_metrics_requests = {};

    this.router = new qdr.Router(this.container.connect());
    var self = this;
    this.connection.on('connection_open', function (context) {
        self.site_id = context.connection.container_id;//TODO: replace with something more robust
    });
}

SiteMetricsAgent.prototype.service_definitions_updated = function (configmaps) {
    var definitions = {};
    if (configmaps && configmaps.length == 1) {
        for (var name in configmaps[0].data) {
            try {
                var record = JSON.parse(configmaps[0].data[name]);
                record.match = get_matcher(record);
                definitions[name] = record;
            } catch (e) {
                log.error('Could not parse service definition for %s as JSON: e', name, e);
            }
        }
        this.build_service_mapping();
    }
    this.services = definitions;
};

SiteMetricsAgent.prototype.build_service_mapping = function () {
    if (this.services && this.current_pods) {
        var mapping = {};//service to list of pods
        for (var address in this.services) {
            var listing = [];
            var service = this.services[address];
            for (var i in this.current_pods) {
                service.match(this.current_pods[i], listing);
            }
            if (listing.length) {
                mapping[address] = listing;
            }
        }
        this.service_mapping = mapping;
        log.info('built service mapping: %j', mapping);
        this.check_ready();
    }
};

SiteMetricsAgent.prototype.wait_until_ready = function () {
    var self = this;
    return new Promise(function (resolve, reject) {
        if (self.address === undefined || self.service_mapping === undefined) {
            log.info('Waiting for metrics agent to be ready');
            self.ready.push(resolve);
        } else {
            log.info('Metrics agent is ready');
            resolve();
        }
    });
};

SiteMetricsAgent.prototype.check_ready = function () {
    if (this.service_mapping && this.address) {
        if (this.ready.length) {
            log.info('Metrics agent now ready');
            while (this.ready.length) {
                this.ready.pop()();
            }
        }
    } else if (this.ready) {
        log.info('Metrics agent not yet ready');
    }
};

SiteMetricsAgent.prototype.pods_updated = function (pods) {
    this.proxies = pods.filter(pod_ready_and_running).filter(is_proxy_pod).map(get_pod_name);
    var latest = pods.map(pod_summary);
    latest.sort(name_compare);
    var changes = myutils.changes(this.current_pods, latest, name_compare, equivalent_pod);
    if (changes) {
        this.current_pods = latest;
        var ip_lookup = this.ip_lookup;
        changes.added.forEach(function (pod) {
            if (pod.ip) ip_lookup[pod.ip] = pod.name;
        });
        changes.removed.forEach(function (pod) {
            delete ip_lookup[pod.ip];
        });
        changes.modified.forEach(function (pod) {
            if (pod.ip) ip_lookup[pod.ip] = pod.name;
            else delete ip_lookup[pod.ip];
        });
        if (this.service_mapping === undefined) {
            this.build_service_mapping();
        }
    }
};

SiteMetricsAgent.prototype.on_receiver_open = function (context) {
    this.address = context.receiver.source.address;
    this.check_ready();
};

SiteMetricsAgent.prototype.on_message = function (context) {
    log.info('site metrics agent received %j', context.message);
    if (context.message.subject === 'bridge-metrics') {
        var request = this.inprogress_bridge_metrics_requests[context.message.correlation_id];
        if (request) {
            request.add_metrics(context.message.group_id, JSON.parse(context.message.body));
            log.info('site metrics agent handled bridge-metrics response from %s', context.message.group_id);
            if (request.is_complete()) {
                delete this.inprogress_bridge_metrics_requests[context.message.correlation_id];
            }
        } else {
            log.error('unexpected bridge-metrics response from %s (%s)', context.message.group_id, context.message.correlation_id);
        }
    } else if (context.message.subject === 'site-metrics') {
        var request = this.inprogress_site_metrics_requests[context.message.correlation_id];
        if (request) {
            request.add_metrics(context.message.group_id, JSON.parse(context.message.body));
            log.info('site metrics agent handled site-metrics response from %s', context.message.group_id);
            if (request.is_complete()) {
                delete this.inprogress_site_metrics_requests[context.message.correlation_id];
            }
        } else {
            log.error('unexpected site-metrics response from %s (%s)', context.message.group_id, context.message.correlation_id);
        }
    } else if (context.message.subject === 'site-metrics-request') {
        var self = this;
        this.wait_until_ready().then(function () {
            self.get_connected_sites().then(function (connected_sites) {
                log.info('connected sites are %j', connected_sites);
                self.collect_proxy_metrics().then(function (results) {
                    var site_metrics = {
                        site_name: self.site_id,
                        site_id: self.origin, //TODO: sort this out
                        connected_sites: connected_sites,
                        targets: self.service_mapping,
                        bridge_metrics: results
                    };
                    context.connection.send({to:context.message.reply_to, group_id:self.site_id, correlation_id:context.message.correlation_id, subject:'site-metrics', body:JSON.stringify(site_metrics)});
                    log.info('site metrics agent sent reply');
                }).catch(function (error) {
                    log.error(error);
                    //TODO: proper error propagation back to caller
                });
            }).catch(function (error) {
                log.error(error);
                //TODO: proper error propagation back to caller
            });
        }).catch(function (error) {
            log.error(error);
            //TODO: proper error propagation back to caller
        });
    }
}

SiteMetricsAgent.prototype.next_request_id = function () {
    return 'request-' + this.counter++;
};

SiteMetricsAgent.prototype.collect_proxy_metrics = function () {
    if (this.proxies.length === 0) return Promise.resolve({});

    //send out request, wait for responses
    var request_id = this.next_request_id();
    this.local_sender.send({subject: 'bridge-metrics-request', reply_to:this.address, correlation_id:request_id});
    log.info('site metrics agent sent bridge-metrics-request');
    //wait for all the results
    var self = this;
    return new Promise(function (resolve, reject) {
        self.inprogress_bridge_metrics_requests[request_id] = new AggregatedBridgeMetrics(self.proxies, self.ip_lookup, resolve, reject);
    });
};

SiteMetricsAgent.prototype.collect = function () {
    var self = this;
    return this.wait_until_ready().then(function () {
        return self.get_site_ids().then(function (ids) {
            if (ids.length === 0) {
                return Promise.resolve({});
            } else {
                //send out request, wait for responses
                var request_id = self.next_request_id();
                self.global_sender.send({subject: 'site-metrics-request', reply_to:self.address, correlation_id:request_id});
                log.info('site metrics agent sent site-metrics-request');
                return new Promise(function (resolve, reject) {
                    self.inprogress_site_metrics_requests[request_id] = new AggregatedSiteMetrics(ids, resolve, reject);
                });
            }
        });
    });
};

SiteMetricsAgent.prototype.get_site_ids = function () {
    return this.router.get_nodes().then(function (nodes) {
        return nodes.map(function (node) {
            return node.id;//TODO: replace this with something more reliable
        });
    });
};

SiteMetricsAgent.prototype.get_connected_sites = function () {
    log.info('calling get_connected_sites()...');
    var self = this;
    return this.router.get_nodes().then(function (nodes) {
        return Promise.all(
            nodes.filter(function (node) {
                return !node.nextHop;
            }).map(function (node) {
                //need to get the metadata field for the router identified by this id
                return self.router.get_router_for_id(node.id);
            })
        ).then(function (results) {
            log.info('Got connected routers: %j', results);
            return results.map(function (routers) {
                return routers[0].metadata;
            });
        });
    });
};

module.exports.create_bridge_metrics = function (address, protocol, origin) {
    if (protocol === 'http') {
        var agent = new BridgeMetricsAgent(origin, http_metrics(address));
        log.info('initialised agent with metrics: %j', agent.metrics);
        return agent.metrics;
    } else {
        //TODO: tcp and http2 metrics
        var agent = new BridgeMetricsAgent(origin, {address: address});
        return undefined;
    }
};

module.exports.http_ingress_request = function (metrics, clientip, method, response_code, bytes_in, bytes_out, latency) {
    if (metrics) {
        record_http_request(metrics.ingress, method, response_code, bytes_in, bytes_out, latency);
        record_indexed_http_request(metrics.ingress.by_client, clientip, method, response_code, bytes_in, bytes_out, latency);
        log.info('http igress request %j', metrics);
    }
};

module.exports.http_egress_request = function (metrics, serverpod, method, response_code, bytes_in, bytes_out, latency) {
    if (metrics) {
        record_http_request(metrics.egress, method, response_code, bytes_in, bytes_out, latency);
        record_indexed_http_request(metrics.egress.by_server, serverpod, method, response_code, bytes_in, bytes_out, latency);
        log.info('http egress request %j', metrics);
    }
};

//to be called by each controller
module.exports.create_server = function (origin, port) {
    var agent = new SiteMetricsAgent(origin);
    var server = http.createServer(function (request, response) {
        agent.collect().then(function (results) {
            response.statusCode = 200;
            response.end(JSON.stringify(results, null, 2));
        }).catch(function (error) {
            response.statusCode = 500;
            response.end("Cause: " + error + "\n\n");
        });
    });
    server.listen(port);
    return server
}
