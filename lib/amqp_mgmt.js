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

var log = require('./log.js').logger();
var rhea = require('rhea');

function match_source_address (link, address) {
    return link && link.local && link.local.attach && link.local.attach.source
        && link.local.attach.source.value[0].toString() === address;
}

function get_attribute_names (objects) {
    var names = {};
    for (var i in objects) {
        var o = objects[i];
        for (var name in o) {
            names[name] = true;
        }
    }
    return Object.keys(names);
}

function get_attribute_values (object, names) {
    return names.map(function (name) {
        return object[name];
    });
}

function convert_query_results (objects, attribute_names) {
    var names = attribute_names || get_attribute_names(objects);
    return {
        attributeNames: names,
        results: objects.map(function (object) {
            return get_attribute_values (object, names)
        })
    };
}

const success_codes = {
    create: 201,
    delete: 204
};

function ManagementServer () {
    this.entities = {};
    this.container = rhea.create_container({id:'bridge-server-' + process.env.HOSTNAME, enable_sasl_external:true})
    var self = this;
    this.container.on('receiver_open', function (context) {
        if (context.receiver.remote.attach.target.address === '$management') {
            context.receiver.set_target({address:context.receiver.remote.attach.target.address});
        } else if (context.receiver !== self.receiver) {
            context.receiver.close();
        }
    });
    this.container.on('sender_open', function (context) {
        if (context.sender.source.dynamic) {
            var id = self.container.generate_uuid();
            context.sender.set_source({address:id});
        }
    });
    this.container.on('message', function (context) {
        var request = context.message;
        var reply_to = request.reply_to;
        var response = {to: reply_to, application_properties:{}};
        response.correlation_id = request.correlation_id;
        if (request.application_properties && request.application_properties.operation) {
            var operation = request.application_properties.operation.toLowerCase();
            if (operation === 'query') {
                var type = request.application_properties.entityType;
                if (type) {
                    var entity = self.entities[type];
                    if (entity) {
                        var objects = entity.query();
                        var names;
                        if (request.body.attributeNames && request.body.attributeNames.length) {
                            names = request.body.attributeNames;
                        }
                        response.body = convert_query_results(objects,  names);
                        response.application_properties.statusCode = 200;
                        response.application_properties.statusDescription = 'OK';
                    } else {
                        response.application_properties.statusCode = 404;
                        response.application_properties.statusDescription = 'No such type: ' + type;
                        console.log('No type %s for %s; types are %j', type, operation, Object.keys(self.entities));
                    }
                } else {
                    response.application_properties.statusCode = 422;
                    response.application_properties.statusDescription = 'No entity type specified';
                }
            } else {
                var type = request.application_properties.type;
                if (type) {
                    var entity = self.entities[type];
                    if (entity) {
                        var method = entity[operation];
                        if (method) {
                            method = method.bind(entity);
                            var name = request.application_properties.name || request.application_properties.identity;
                            try {
                                response.body = method(name, request.body);
                                response.application_properties.statusCode = success_codes[operation] || 200;
                                response.application_properties.statusDescription = 'OK';
                            } catch (e) {
                                response.application_properties.statusCode = 400;
                                response.application_properties.statusDescription = 'Bad request: ' + e;
                            }
                        } else {
                            response.application_properties.statusCode = 501;
                            response.application_properties.statusDescription = operation + ' not supported for ' + request.application_properties.type;
                        }
                    } else {
                        response.application_properties.statusCode = 404;
                        response.application_properties.statusDescription = 'No such type: ' + type;
                        console.log('No such type for %s; types are %j', operation, Object.keys(self.entities));
                    }
                } else {
                    response.application_properties.statusCode = 422;
                    response.application_properties.statusDescription = 'No type specified';
                }
            }
        } else {
            response.application_properties.statusCode = 422;
            response.application_properties.statusDescription = 'No operation specified';
        }
        if (context.receiver === self.receiver) {
            context.connection.send(response);
        } else {
            var o = context.connection.find_sender(function (s) { return match_source_address(s, reply_to); });
            if (o) {
                o.send(response);
            }
        }
    });
}

ManagementServer.prototype.listen = function (port) {
    this.server = this.container.listen({port: port});
};

ManagementServer.prototype.connect = function () {
    this.connection = this.container.connect();
    var self = this;
    this.connection.on('connection_open', function (context) {
        self.receiver = context.connection.open_receiver(context.connection.container_id + "/bridge-server/$management");
    });
};

ManagementServer.prototype.register = function (type, manager) {
    this.entities[type] = manager;
};

module.exports.server = function () {
    return new ManagementServer();
}
