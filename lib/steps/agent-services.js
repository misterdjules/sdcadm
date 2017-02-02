/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2017 Joyent, Inc.
 */

/*
 * Steps for ensuring that SAPI has all the core type=agent services.
 * By "core" agents, we mean those installed by default on node setup
 * (which currently is those in the agentsshar) -- with the exception of the
 * marlin agent.
 */

var assert = require('assert-plus');
var fs = require('fs');
var mod_uuid = require('node-uuid');
var util = require('util');
var vasync = require('vasync');

var errors = require('../errors');

var DRY_RUN = false; // An off-switch for dev/testing.


/**
 * Ensure that SAPI has a service entry for the core agents.
 *
 * Note on history: If changes are made, this will add a SAPI history item.
 * However, because the current sdcadm `History` API isn't that convenient
 * for building up changes for a single history item, we will NOT use the
 * `sdcadm.uuid` for this run. Doing so can easily result in the this history
 * item getting overwritten by a separate `sdcadm.history.saveHistory`
 * during this same command. It would be good to improve this at some point.
 */
function agentServicesEnsureCreated(arg, cb) {
    assert.object(arg, 'arg');
    assert.func(arg.progress, 'arg.progress');
    assert.object(arg.log, 'arg.log');
    assert.object(arg.sdcadm, 'arg.sdcadm');
    assert.func(cb, 'cb');

    var log = arg.log.child({component: 'agentServicesEnsureCreated'}, true);
    var sdcadm = arg.sdcadm;
    var progress = arg.progress;

    // We need at least a MIN_VALID_SAPI_VERSION image so
    // type=agent suport is there.
    var MIN_VALID_SAPI_VERSION = '20140703';
    var app = sdcadm.sdc;
    var historyItem = null;

    var img;
    var agentNames = [
        'agents_core',
        'amon-agent',
        'amon-relay',
        'cabase',
        'cainstsvc',
        'cmon-agent',
        'cn-agent',
        'config-agent',
        'firewaller',
        'hagfish-watcher',
        'net-agent',
        'smartlogin',
        'vm-agent'
    ];
    var agentServices = {};
    agentNames.forEach(function (n) {
        var logLevelKey = n.toUpperCase().replace('-', '_') + '_LOG_LEVEL';
        agentServices[n] = {
            type: 'agent',
            params: {
                tags: {
                    smartdc_role: n,
                    smartdc_type: 'core'
                }
            },
            metadata: {
                SERVICE_NAME: n
            },
            manifests: {
            }
        };

        agentServices[n].metadata[logLevelKey] = 'info';
    });


    // The first time we add agent services to SAPI we'll use the HN image
    // version to create the service, assuming that's the version installed
    // everywhere across the whole SDC setup
    function getAgentImages(callback) {
        vasync.forEachPipeline({
            func: function (agent, next) {
                var name = agent.name;
                var imgUUIDPath = util.format(
                    '/opt/smartdc/agents/lib/node_modules/%s/image_uuid',
                    name);
                fs.readFile(imgUUIDPath, {
                    encoding: 'utf8'
                }, function (err, data) {
                    if (err) {
                        log.error({err: err}, 'Error reading agent image uuid');
                        next();
                        return;
                    }
                    agentServices[name].params.image_uuid = data.trim();
                    next();
                });
            },
            inputs: agentNames.map(function (agent) {
                return agentServices[agent];
            })
        }, callback);
    }

    var newAgentServices = [];
    var updateAgentServices = [];

    vasync.pipeline({funcs: [
        function getSapiVmImgs(_, next) {
            sdcadm.getImgsForSvcVms({
                svc: 'sapi'
            }, function (err, obj) {
                if (err) {
                    return next(err);
                }
                img = obj.imgs[0];
                return next();
            });
        },
        function checkMinSapiVersion(_, next) {
            var splitVersion = img.version.split('-');
            var validSapi = false;

            if (splitVersion[0] === 'master') {
                validSapi = splitVersion[1].substr(0, 8) >=
                    MIN_VALID_SAPI_VERSION;
            } else if (splitVersion[0] === 'release') {
                validSapi = splitVersion[1] >= MIN_VALID_SAPI_VERSION;
            }

            if (!validSapi) {
                return next(new errors.SDCClientError(new Error('Datacenter ' +
                    'does not have the minimum SAPI version needed for adding' +
                    ' service agents. ' +
                    'Please try again after upgrading SAPI')));
            }

            return next();
        },

        function checkExistingAgents(_, next) {
            vasync.forEachParallel({
                func: function checkAgentExist(agent, callback) {
                    sdcadm.sapi.listServices({
                        name: agent,
                        type: 'agent',
                        application_uuid: app.uuid
                    }, function (svcErr, svcs) {
                        if (svcErr) {
                            return callback(svcErr);
                        }
                        if (!svcs.length) {
                            newAgentServices.push(agent);
                        } else if (!svcs[0].params.image_uuid) {
                            agentServices[agent] = svcs[0];
                            updateAgentServices.push(agent);
                        }
                        return callback();
                    });
                },
                inputs: Object.keys(agentServices)
            }, next);
        },

        function getAgentImgVersions(_, next) {
            getAgentImages(next);
        },

        function saveChangesToHistory(_, next) {
            var changes = [];
            newAgentServices.forEach(function (s) {
                changes.push({
                    service: {
                        name: s,
                        type: 'agent'
                    },
                    type: 'create-service'
                });
            });

            updateAgentServices.forEach(function (s) {
                changes.push({
                    service: {
                        name: s,
                        type: 'agent'
                    },
                    type: 'update-service'
                });
            });

            if (changes.length) {
                sdcadm.history.saveHistory({
                    uuid: mod_uuid.v4(),
                    changes: changes
                }, function (err, historyItem_) {
                    if (err) {
                        next(err);
                        return;
                    }
                    historyItem = historyItem_;
                    next();
                });
            } else {
                next();
            }
        },
        function addAgentsServices(_, next) {
            vasync.forEachParallel({
                inputs: newAgentServices,
                func: function addAgentSvc(agent, callback) {
                    progress('Adding service for agent \'%s\'', agent);
                    log.trace({
                        service: agent,
                        params: agentServices[agent]
                    }, 'Adding new agent service');
                    if (DRY_RUN) {
                        callback();
                    } else {
                        sdcadm.sapi.createService(agent, app.uuid,
                            agentServices[agent], callback);
                    }
                }
            }, next);
        },

        function updateAgentsServices(_, next) {
            vasync.forEachParallel({
                inputs: updateAgentServices,
                func: function updateAgentSvc(agent, callback) {
                    progress('Updating service for agent \'%s\'', agent);
                    log.trace({
                        service: agent,
                        params: agentServices[agent]
                    }, 'Updating agent service');
                    if (DRY_RUN) {
                        callback();
                    } else {
                        sdcadm.sapi.updateService(agentServices[agent].uuid, {
                            params: agentServices[agent].params
                        }, callback);
                    }
                }
            }, next);
        }
    ]}, function (err) {
        if (historyItem) {
            if (err) {
                historyItem.error = err;
            }
            sdcadm.history.updateHistory(historyItem, function (histErr) {
                cb(err || histErr);
            });
        } else {
            cb(err);
        }
    });
}


//---- exports

module.exports = {
    agentServicesEnsureCreated: agentServicesEnsureCreated
};

// vim: set softtabstop=4 shiftwidth=4: