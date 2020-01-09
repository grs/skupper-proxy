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

var server = http.createServer(function (request, response) {
    console.log('%s %s on %s', request.method, request.url, request.headers.host);
    response.statusCode = 200;
    response.end(request.method + ' ' + request.url + ' from ' + request.socket.remoteAddress + ' handled by ' + process.env.HOSTNAME + '\n');
});
server.listen(process.env.PORT || 8080, function () {
    console.log('listening on %s', server.address().port);
});
