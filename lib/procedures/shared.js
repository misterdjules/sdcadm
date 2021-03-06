/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2016 Joyent, Inc.
 */

var path = require('path');
var fs = require('fs');
var util = require('util'),
    format = util.format;
var child_process = require('child_process'),
    execFile = child_process.execFile,
    spawn = child_process.spawn;
var vasync = require('vasync');
var assert = require('assert-plus');
var vmadm = require('../vmadm');
var svcadm = require('../svcadm');
var common = require('../common');
var errors = require('../errors'),
    InternalError = errors.InternalError,
    SDCClientError = errors.SDCClientError;



/**
 * Get past HEAD-1804 where we changed to a common user-script
 * (that shall not change again).
 *
 * Note: sdcadm's "etc/setup/user-script" is a copy of
 * "usb-headnode.git:defaults/user-script.common". At the time of
 * writing the latter is canonical. Eventually, when we have
 * "sdcadm setup", the former will be canonical.
 */
function getUserScript(arg, next) {
    if (arg.userScript) {
        return next();
    }
    var userScriptPath = path.resolve(__dirname, '..', '..',
            'etc', 'setup', 'user-script');
    fs.readFile(userScriptPath, 'utf8', function (err, content) {
        arg.userScript = content;
        next(err);
    });
}


/**
 * In case of rollback, we need to get the old user script from the previously
 * made backup and proceed exactly the same way.
 */
function getOldUserScript(arg, next) {
    if (arg.userScript) {
        return next();
    }
    var svc = arg.change.service;
    var img = (arg.change.inst) ? arg.change.inst.image :
        arg.change.insts[0].image;
    var log = arg.opts.log;

    var usPath = path.resolve(arg.opts.upDir,
        format('%s.%s.user-script', svc.uuid, img));

    log.debug({usPath: usPath, service: svc.name},
        'looking for old user-script for possible rollback');

    fs.exists(usPath, function (exists) {
        if (!exists) {
            if (svc.metadata) {
                arg.userScript = svc.metadata['user-script'];
            }
            return next();
        }

        fs.readFile(usPath, 'utf8', function (err, content) {
            arg.userScript = content;
            next(err);
        });
    });
}

function writeOldUserScriptForRollback(arg, next) {
    var svc = arg.change.service;
    var img = arg.change.image;
    var log = arg.opts.log;
    if (!svc.metadata ||
            svc.metadata['user-script'] === arg.userScript) {
        return next();
    }
    var usPath = path.resolve(arg.opts.wrkDir,
        format('%s.%s.user-script', svc.uuid, img.uuid));
    log.debug({usPath: usPath, service: svc.name},
        'save old user-script for possible rollback');
    fs.writeFile(usPath,
        svc.metadata['user-script'],
        'utf8',
        function (err) {
            if (err) {
                return next(new errors.UpdateError(err,
                    'error saving old user-script: ' + usPath));
            }
            next();
        });
}

function updateSvcUserScript(arg, next) {
    var svc = arg.change.service;
    var progress = arg.opts.progress;
    var sdcadm = arg.opts.sdcadm;
    if (svc.metadata['user-script'] === arg.userScript) {
        return next();
    }
    progress('Update "%s" service user-script', svc.name);
    sdcadm.sapi.updateService(
        arg.change.service.uuid,
        {
            params: {
                'user-script': arg.userScript
            }
        },
        errors.sdcClientErrWrap(next, 'sapi'));
}

function updateVmUserScript(arg, next) {
    var svc = arg.change.service;
    var progress = arg.opts.progress;
    var inst = arg.change.inst;
    var log = arg.opts.log;
    if (svc.metadata && svc.metadata['user-script'] === arg.userScript) {
        return next();
    }
    progress('Update "%s" VM %s user-script', svc.name, inst.zonename);
    log.trace({inst: inst, image: arg.change.image.uuid},
        'reprovision VM inst');
    var child = spawn('/usr/sbin/vmadm', ['update', inst.zonename]);
    var stdout = [];
    var stderr = [];
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', function (s) {
        stdout.push(s);
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function (s) {
        stderr.push(s);
    });
    child.on('close', function vmadmDone(code, signal) {
        stdout = stdout.join('');
        stderr = stderr.join('');
        log.debug({inst: inst, image: arg.change.image.uuid,
            code: code, signal: signal,
            stdout: stdout, stderr: stderr},
            'updated VM inst');
        if (code || signal) {
            var msg = format(
                'error update VM %s user-script: ' +
                'exit code %s, signal %s\n' +
                '    stdout:\n%s' +
                '    stderr:\n%s',
                inst.zonename, code, signal,
                common.indent(stdout, '        '),
                common.indent(stderr, '        '));
            return next(new errors.InternalError({message: msg}));
        }
        next();
    });
    child.stdin.setEncoding('utf8');
    child.stdin.write(JSON.stringify({
        set_customer_metadata: {
            'user-script': arg.userScript
        }
    }));
    child.stdin.end();
}

function updateSapiSvc(arg, next) {
    var sdcadm = arg.opts.sdcadm;
    sdcadm.sapi.updateService(
        arg.change.service.uuid,
        {
            params: {
                image_uuid: arg.change.image.uuid
            }
        },
        errors.sdcClientErrWrap(next, 'sapi'));
}

function imgadmInstall(arg, next) {
    var progress = arg.opts.progress;
    var img = arg.change.image;
    var log = arg.opts.log;
    progress('Installing image %s\n    (%s@%s)',
        img.uuid, img.name, img.version);

    var argv = ['/usr/sbin/imgadm', 'import', '-q', img.uuid];

    var env = common.objCopy(process.env);
    // Get 'debug' level logging in imgadm >=2.6.0 without
    // triggering trace level logging in imgadm versions before
    // that. Trace level logging is too much here.
    env.IMGADM_LOG_LEVEL = 'debug';
    var execOpts = {
        encoding: 'utf8',
        env: env
    };
    log.trace({argv: argv}, 'installing VM image');
    execFile(argv[0], argv.slice(1), execOpts,
        function (err, stdout, stderr) {
            if (err) {
                var msg = format(
                    'error importing VM image %s:\n' +
                    '\targv: %j\n' +
                    '\texit status: %s\n' +
                    '\tstdout:\n%s\n' +
                    '\tstderr:\n%s', img.uuid,
                    argv, err.code, stdout.trim(), stderr.trim());
                return next(new errors.InternalError({
                    message: msg,
                    cause: err
                }));
            }
            next();
        });
}

/**
 *  echo '{}' | json -e "this.image_uuid = '${image_uuid}'" |
 *      vmadm reprovision ${instance_uuid}
 */
function reprovision(arg, next) {
    var progress = arg.opts.progress;
    var inst = arg.change.inst;
    var log = arg.opts.log;
    // TODO(trent): refactor this into ./lib/vmadm.js
    progress('Reprovisioning %s VM %s', inst.service, inst.zonename);
    log.trace({inst: inst, image: arg.change.image.uuid},
        'reprovision VM inst');
    var child = spawn('/usr/sbin/vmadm',
        ['reprovision', inst.zonename]);
    var stdout = [];
    var stderr = [];
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', function (s) {
        stdout.push(s);
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function (s) {
        stderr.push(s);
    });
    child.on('close', function vmadmDone(code, signal) {
        stdout = stdout.join('');
        stderr = stderr.join('');
        log.debug({inst: inst, image: arg.change.image.uuid,
            code: code, signal: signal, stdout: stdout,
            stderr: stderr},
            'reprovisioned VM inst');
        if (code || signal) {
            var msg = format(
                'error reprovisioning VM %s: ' +
                'exit code %s, signal %s\n' +
                '    stdout:\n%s' +
                '    stderr:\n%s',
                inst.zonename, code, signal,
                common.indent(stdout, '        '),
                common.indent(stderr, '        '));
            return arg.opts.sdcadm.reprovFailLock_Lock(msg, function (err) {
                if (err) {
                    log.error(err, 'reprovFailLock_Lock failure');
                }
                next(new errors.InternalError({message: msg}));
            });
        }
        next();
    });
    child.stdin.setEncoding('utf8');
    child.stdin.write(JSON.stringify({
        image_uuid: arg.change.image.uuid
    }));
    child.stdin.end();
}

function waitForInstToBeUp(arg, cb) {
    var progress = arg.opts.progress;
    var sdcadm = arg.opts.sdcadm;
    var inst = arg.change.inst;
    var uuid = (arg.tmpUUID) ? arg.tmpUUID: inst.instance;
    var server = inst.server;

    var counter = 0;
    var limit = 60;

    // 1 - Check the VM is running:
    var running = false;
    // 2 - Check for instance services errors
    function _waitForInstance() {
        counter += 1;
        vasync.pipeline({
            funcs: [
                function checkInstIsRunning(_, next) {
                    if (running) {
                        return next();
                    }
                    sdcadm.cnapi.getVm(server, uuid, function (err, vm) {
                        if (err) {
                            arg.opts.log.debug({
                                err: err
                            }, 'checkInstIsRunning');
                            next(new errors.SDCClientError(err, 'vmapi'));
                        } else {
                            arg.opts.log.debug({vm: vm}, 'checkInstIsRunning');
                            if (vm.state === 'running') {
                                running = true;
                            }
                            next();
                        }
                    });
                },
                function checkInstSvcs(_, next) {
                    if (!running) {
                        return next();
                    }

                    arg.opts.sdcadm.checkHealth({
                        insts: [inst]
                    }, function (err, results) {
                        if (err) {
                            arg.opts.log.debug({
                                err: err
                            }, 'checkInstSvcs');
                            return next(err);
                        }
                        arg.opts.log.debug({
                            results: results
                        }, 'checkInstSvcs');
                        var res = results[0];
                        if (res.health_errors && res.health_errors.length) {
                            return next(new errors.InstanceIsDownError(
                                        res.health_errors[0].message));
                        }
                        return next();
                    });
                }
            ]
        }, function (err) {
            if (err || !running) {

                // If any service went into maintenance we rather fail now:
                var hErrs = inst.health_errors;
                if (hErrs && hErrs.length && hErrs[0].message &&
                    hErrs[0].message.match(/State\: maintenance/)) {
                    return cb(new errors.InstanceIsDownError(
                                hErrs[0].message));
                }

                if (counter < limit) {
                    // Cleanup errors for next iteration, or it'll fail again:
                    delete inst.health_errors;
                    return setTimeout(_waitForInstance, 5000);
                } else {
                    return cb(new errors.InstanceIsDownError(format(
                        'Timeout (5m) waiting for %s instance %s ' +
                        'to come up', inst.service, inst.zonename)));
                }
            } else {
                return cb(null);
            }
        });
    }
    progress('Waiting for %s instance %s to come up',
            inst.service, inst.zonename);
    _waitForInstance();
}


// --- Used by upgrades going through temp instances:

function checkHA(arg, next) {
    var progress = arg.opts.progress;
    var sdcadm = arg.opts.sdcadm;
    var svc = arg.change.service;

    progress('Verifying if we are on an HA setup');

    sdcadm.sapi.listInstances({
        service_uuid: svc.uuid
    }, function (err, instances) {
        if (err) {
            return next(new SDCClientError(err, 'sapi'));
        }

        sdcadm.vmapi.listVms({
            'tag.smartdc_role': arg.change.service.name,
            state: 'running'
        }, function (vmsErr, vms) {
            if (vmsErr) {
                return next(new SDCClientError(vmsErr, 'vmapi'));
            }

            sdcadm.log.trace({vms: vms, instances: instances}, 'checkHA');

            vms = vms.map(function (vm) {
                return (vm.uuid);
            });

            var errs = [];
            instances.forEach(function checkVm(ins) {
                if (vms.indexOf(ins.uuid) === -1) {
                    errs.push(format('%s (%s)', ins.uuid, ins.params.alias));
                }
            });

            if (errs.length) {
                return next(new errors.InternalError({
                    message: format('The following SAPI instances are not ' +
                        'present into VMAPI and should be removed before ' +
                        'continue with the upgrade process:\n%s',
                        errs.join(', '))
                }));
            }

            if (instances.length > 1) {
                arg.HA = true;
            }
            return next();
        });
    });
}


function provisionTmpVm(arg, next) {
    if (arg.HA) {
        return next();
    }
    var progress = arg.opts.progress;
    var sdcadm = arg.opts.sdcadm;
    var inst = arg.change.inst;
    var log = arg.opts.log;
    var svc = arg.change.service;

    progress('Provisioning Temporary %s VM %s', inst.service,
        arg.tmpAlias);
    log.trace({
        alias: arg.tmpAlias,
        image: arg.change.image.uuid,
        server: arg.server_uuid
    }, 'Provisioning temporary VM inst');
    sdcadm.sapi.createInstance(svc.uuid, {
        params: {
            alias: arg.tmpAlias,
            server_uuid: arg.server_uuid
        }
    }, function (err, body) {
        if (err) {
            return next(err);
        }
        arg.tmpUUID = body.uuid;
        return next();
    });
}


function waitForTmpInstToBeUp(arg, next) {
    if (arg.HA) {
        return next();
    }
    return waitForInstToBeUp(arg, next);
}


function getTmpInstanceUUID(arg, next) {
    if (arg.HA) {
        return next();
    }
    var progress = arg.opts.progress;
    var log = arg.opts.log;
    progress('Running vmadm lookup to get tmp instance UUID');
    var argv = [
        '/usr/sbin/vmadm',
        'lookup',
        '-1',
        'alias=' + arg.tmpAlias
    ];
    common.execFilePlus({
        argv: argv,
        log: log
    }, function (err, stdout, stderr) {
        if (err) {
            next(err);
        } else {
            arg.tmpUUID = stdout.trim();
            log.debug('Tmp instance found: %s', arg.tmpUUID);
            next();
        }
    });
}

function checkIfTmpVMHasErrors(arg, next) {
    if (arg.HA) {
        return next();
    }
    var progress = arg.opts.progress;
    var log = arg.opts.log;
    progress('Checking for service errors in temporary instance %s',
        arg.tmpUUID);
    var counter = 0;
    var limit = 60;
    function _checkIfItHasErrors() {
        counter += 1;
        var argv = ['/usr/bin/svcs', '-z', arg.tmpUUID, '-x'];
        common.execFilePlus({
            argv: argv,
            log: log
        }, function (err, stdout, stderr) {
            if (err) {
                return next(err);
            }
            var errs = stdout.trim();
            if (errs) {
                if (counter < limit) {
                    return setTimeout(_checkIfItHasErrors, 5000);
                } else {
                    progress('Timeout (5m) waiting for %s to be up',
                            arg.tmpUUID);
                    return next(errs);
                }
            }
            return next();
        });
    }
    _checkIfItHasErrors();
}

function disableVMRegistrar(arg, next) {
    var progress = arg.progress;
    var zonename = arg.zonename;
    var log = arg.log;
    progress('Disabling registrar on VM %s', zonename);
    svcadm.svcadmDisable({
        fmri: 'registrar',
        zone: zonename,
        wait: true,
        log: log
    }, next);
}

function waitUntilVMNotInDNS(arg, next) {
    var progress = arg.progress;
    var zonename = arg.zonename;
    var server = arg.server;
    progress('Wait until VM %s is out of DNS', zonename);
    common.waitUntilZoneOutOfDNS({
        uuid: zonename,
        server: server,
        alias: arg.alias,
        domain: arg.domain,
        log: arg.log
    }, next);
}

function waitUntilVmInDNS(arg, next) {
    var progress = arg.progress;
    var zonename = arg.zonename;
    var server = arg.server;
    progress('Waiting until %s instance is in DNS', zonename);
    common.waitUntilZoneInDNS({
        uuid: zonename,
        server: server,
        alias: arg.alias,
        domain: arg.domain,
        log: arg.log
    }, next);
}

function stopTmpVm(arg, next) {
    if (arg.HA) {
        return next();
    }
    var progress = arg.opts.progress;
    var log = arg.opts.log;
    progress('Stop tmp VM %s', arg.tmpUUID);
    vmadm.vmStop(arg.tmpUUID, {
        log: log
    }, next);
}

function destroyTmpVM(arg, next) {
    if (arg.HA) {
        return next();
    }
    var progress = arg.opts.progress;
    var sdcadm = arg.opts.sdcadm;
    progress('Destroying tmp VM %s (%s)', arg.tmpUUID, arg.tmpAlias);
    sdcadm.sapi.deleteInstance(arg.tmpUUID, function (err) {
        if (err) {
            var msg = format('Error destroying tmp VM %s (%s)',
                    arg.tmpUUID, arg.tmpAlias);
            return next(new errors.InternalError({
                message: msg,
                cause: err
            }));
        }
        return next();
    });
}

function createInstance(arg, next) {
    var progress = arg.opts.progress;
    var sdcadm = arg.opts.sdcadm;

    progress('Creating "%s" instance', arg.alias);
    var iOpts = {
        params: {
            alias: arg.alias,
            server_uuid: arg.change.server
        },
        metadata: {}
    };

    var svc = arg.change.service.uuid;
    sdcadm.sapi.createInstance(svc, iOpts, function (err, inst_) {
        if (err) {
            return next(
                new errors.SDCClientError(err, 'sapi'));
        }
        progress('Instance "%s" (%s) created',
            inst_.uuid, inst_.params.alias);

        arg.change.inst = {
            alias: arg.alias,
            service: arg.change.service.name,
            zonename: inst_.uuid,
            uuid: inst_.uuid,
            instance: inst_.uuid,
            server: arg.change.server,
            type: arg.change.service.type
        };
        return next();
    });
}

// Functions operating remotely through sdc-oneachnode:

// Same than imgadmInstall but through sdc-oneachnode
function imgadmInstallRemote(opts, callback) {
    var server = opts.server;
    var img = opts.img;
    var progress = opts.progress;
    var log = opts.log;

    progress('Installing image %s\n    (%s@%s)',
        img.uuid, img.name, img.version);

    var argv = [
        '/opt/smartdc/bin/sdc-oneachnode',
        format('-n %s ', server),
        format('/usr/sbin/imgadm import -q %s', img.uuid)
    ];

    var env = common.objCopy(process.env);
    // Get 'debug' level logging in imgadm >=2.6.0 without
    // triggering trace level logging in imgadm versions before
    // that. Trace level logging is too much here.
    env.IMGADM_LOG_LEVEL = 'debug';
    var execOpts = {
        encoding: 'utf8',
        env: env
    };
    log.trace({argv: argv}, 'installing VM image');
    execFile(argv[0], argv.slice(1), execOpts, function (err, stdout, stderr) {
        if (err) {
            var msg = format(
                'error importing VM image %s:\n' +
                '\targv: %j\n' +
                '\texit status: %s\n' +
                '\tstdout:\n%s\n' +
                '\tstderr:\n%s', img.uuid,
                argv, err.code, stdout.trim(), stderr.trim());
            return callback(new errors.InternalError({
                message: msg,
                cause: err
            }));
        }
        callback();
    });
}

// Disable registrar using sdc-oneachnode
function disableVMRegistrarRemote(arg, callback) {
    var progress = arg.progress;
    var zonename = arg.zonename;
    var log = arg.log;
    var server = arg.server;

    progress('Disabling registrar on VM %s (Server: %s)', zonename, server);

    var argv = [
        '/opt/smartdc/bin/sdc-oneachnode',
        format('-n %s ', server),
        format('/usr/sbin/svcadm -z %s disable registrar', zonename)
    ];

    var env = common.objCopy(process.env);
    log.trace({argv: argv}, 'Disabling VM registrar');
    var execOpts = {
        encoding: 'utf8',
        env: env
    };

    execFile(argv[0], argv.slice(1), execOpts, function (err, stdout, stderr) {
        if (err) {
            var msg = format(
                'error disabling VM registrar %s:\n' +
                '\targv: %j\n' +
                '\texit status: %s\n' +
                '\tstdout:\n%s\n' +
                '\tstderr:\n%s', zonename,
                argv, err.code, stdout.trim(), stderr.trim());
            return callback(new errors.InternalError({
                message: msg,
                cause: err
            }));
        }
        callback();
    });
}

// Reprovision through sdc-oneachnode
function reprovisionRemote(opts, callback) {
    common.assertStrictOptions('reprovisionRemote', opts, {
        server: 'uuid',
        img: 'object',
        zonename: 'uuid',
        progress: 'func',
        log: 'object',
        sdcadm: 'object'
    });
    assert.uuid(opts.img.uuid, 'img.uuid');
    assert.func(callback, 'callback');

    var log = opts.log.child({
        inst: opts.zonename,
        image: opts.img.uuid,
        server: opts.server
    });

    opts.progress('Reprovisioning %s VM %s', opts.zonename, opts.server);
    log.trace('reprovision VM inst');

    /*
     * Construct a JSON string that we can embed in single quotes (') in a
     * shell script.
     */
    var reproJSON = JSON.stringify({
        image_uuid: opts.img.uuid
    }).replace(/'/g, '\'"\'"\'');

    var child = spawn('/opt/smartdc/bin/sdc-oneachnode', [
        '-n',
        opts.server,
        format('/usr/sbin/vmadm reprovision %s <<< \'%s\'', opts.zonename,
            reproJSON)
    ]);

    /*
     * Collect stdout and stderr from the child process.
     */
    var stdout = '';
    var stderr = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', function (so) {
        stdout += so;
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function (se) {
        stderr += se;
    });

    child.on('close', function vmadmDone(code, signal) {
        log.debug({
            execResult: {
                code: code,
                signal: signal,
                stdout: stdout,
                stderr: stderr
            }
        }, 'reprovisioned VM inst');

        if (code === 0 && signal === null) {
            /*
             * Execution was successful.
             */
            callback();
            return;
        }

        /*
         * Report the execution failure to our caller and take the
         * reprovision failure lock.
         */
        var msg = [
            format('error reprovisioning VM %s: exit code %s, signal %s',
                opts.zonename, code, signal),
            common.indent('stdout:', 4),
            common.indent(stdout, 8),
            common.indent('stderr:', 4),
            common.indent(stderr, 8)
        ].join('\n');
        opts.sdcadm.reprovFailLock_Lock(msg, function (err) {
            if (err) {
                log.error(err, 'reprovFailLock_Lock failure');
            }
            callback(new errors.InternalError({
                message: msg
            }));
        });
    });
}

// Same than updateVmUserScript, but using sdc-oneachnode
function updateVmUserScriptRemote(arg, next) {
    var svc = arg.service;
    var progress = arg.progress;
    var zonename = arg.zonename;
    var log = arg.log;
    var server = arg.server;
    if (svc.metadata['user-script'] === arg.userScript) {
        return next();
    }
    progress('Update "%s" VM %s user-script', svc.name, zonename);
    log.trace({inst: zonename, userScript: arg.userScript},
        'Update User Script');
    var child = spawn('/opt/smartdc/bin/sdc-oneachnode', [
        format('-n %s ', server),
        'echo \'' +  JSON.stringify({
            set_customer_metadata: {
                /* JSSTYLED */
                'user-script': arg.userScript.replace(/'/g, '"')
            }
        })  + '\'|' +
        format('/usr/sbin/vmadm update %s ', zonename)
    ]);
    var stdout = [];
    var stderr = [];
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', function (s) {
        stdout.push(s);
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function (s) {
        stderr.push(s);
    });
    child.on('close', function vmadmDone(code, signal) {
        stdout = stdout.join('');
        stderr = stderr.join('');
        log.debug({inst: zonename, userScript: arg.userScript,
            code: code, signal: signal,
            stdout: stdout, stderr: stderr},
            'Updated user script');
        if (code || signal) {
            var msg = format(
                'error update VM %s user-script: ' +
                'exit code %s, signal %s\n' +
                '    stdout:\n%s' +
                '    stderr:\n%s',
                zonename, code, signal,
                common.indent(stdout, '        '),
                common.indent(stderr, '        '));
            return next(new errors.InternalError({message: msg}));
        }
        next();
    });
}

function ensureDelegateDataset(arg, next) {
    var progress = arg.progress;
    var zonename = arg.zonename;
    var log = arg.log;
    var server = arg.server;
    var service = arg.service;
    var expectedDs = format('zones/%s/data', zonename);
    var env = common.objCopy(process.env);
    var execOpts = {
        encoding: 'utf8',
        env: env
    };

    function createDataset(_, nextCb) {

        var argv = [
            '/opt/smartdc/bin/sdc-oneachnode',
            format('-n %s ', server),
            format('/usr/sbin/zfs create %s', expectedDs)
        ];

        log.trace({argv: argv}, 'Creating ZFS dataset');
        execFile(argv[0], argv.slice(1), execOpts, function (err, stdo, stde) {
            if (err) {
                var msg = format(
                    'error adding delegate dataset %s:\n' +
                    '\targv: %j\n' +
                    '\texit status: %s\n' +
                    '\tstdout:\n%s\n' +
                    '\tstderr:\n%s', zonename,
                    argv, err.code, stdo.trim(), stde.trim());
                return nextCb(new errors.InternalError({
                    message: msg,
                    cause: err
                }));
            }
            log.debug('zfs dataset created: %s', stdo.toString());
            nextCb();
        });
    }

    function setZonedOn(_, nextCb) {
        var argv = [
            '/opt/smartdc/bin/sdc-oneachnode',
            format('-n %s ', server),
            format('/usr/sbin/zfs set zoned=on %s', expectedDs)
        ];
        log.trace({argv: argv}, 'Setting zoned=on for ZFS dataset');
        execFile(argv[0], argv.slice(1), execOpts, function (err, stdo, stde) {
            if (err) {
                var msg = format(
                    'error disabling VM registrar %s:\n' +
                    '\targv: %j\n' +
                    '\texit status: %s\n' +
                    '\tstdout:\n%s\n' +
                    '\tstderr:\n%s', zonename,
                    argv, err.code, stdo.trim(), stde.trim());
                return nextCb(new errors.InternalError({
                    message: msg,
                    cause: err
                }));
            }
            log.debug('zfs dataset set zoned=on: %s', stdo.toString());
            nextCb();
        });
    }

    function zonecfgAddDataset(_, nextCb) {
        var argv = [
            '/opt/smartdc/bin/sdc-oneachnode',
            format('-n %s ', server),
            format('/usr/sbin/zonecfg -z %s "add dataset; set name=%s; end"',
                    zonename, expectedDs)
        ];
        log.trace({argv: argv}, 'Setting ZFS dataset name');
        execFile(argv[0], argv.slice(1), execOpts, function (err, stdo, stde) {
            if (err) {
                var msg = format(
                    'error disabling VM registrar %s:\n' +
                    '\targv: %j\n' +
                    '\texit status: %s\n' +
                    '\tstdout:\n%s\n' +
                    '\tstderr:\n%s', zonename,
                    argv, err.code, stdo.trim(), stde.trim());
                return nextCb(new errors.InternalError({
                    message: msg,
                    cause: err
                }));
            }
            log.debug('zonecfg set dataset name: %s', stdo.toString());
            nextCb();
        });
    }


    function addDelegateDataset() {
        vasync.pipeline({funcs: [
            createDataset,
            setZonedOn,
            zonecfgAddDataset
        ], arg: arg}, next);
    }

    common.vmGetRemote({
        uuid: zonename,
        server: server,
        log: log
    }, function (err, vm) {
        if (err) {
            return next(err);
        }
        if (!vm.datasets || vm.datasets.indexOf(expectedDs) === -1) {
            progress('Adding a delegate dataset to "%s" VM %s', service.name,
                zonename);
            return addDelegateDataset();
        }

        progress('"%s" VM already has a delegate dataset', service.name);
        next();
    });
}


// TODO(pedro): This is a dupe from procedures/update-manatee-v2.js
/**
 * Get Manatee Shard status using manatee-adm into the given manatee VM, which
 * may be on any server, not only the one running sdcadm command.
 *
 * @param {Object} opts: should include:
 *      - manateeUUID {String}: UUID of the manatee VM where we want to execute
 *          manatee-adm in order to check the current shard status
 *      - server {String}: UUID of the server containing the aformentioned
 *          manatee VM
 *      - leaderIP {String}: IP of the ZK leader to be used instead of ENV vars
 *      - log {Bunyan Logger}.
 * @param {Function} callback: of the form f(err, shard).
 */
function getShardStatus(opts, callback) {
    var cmd = 'status';
    if (opts.leaderIP) {
        cmd += ' -z ' + opts.leaderIP + ':2181';
    }

    common.manateeAdmRemote({
        server: opts.server,
        vm: opts.manateeUUID,
        cmd: cmd,
        log: opts.log
    }, function (err, res, stderr) {
        if (err) {
            return callback(err);
        }

        var manateeShard = JSON.parse(res);
        return callback(null, manateeShard);
    });
}

/**
 * Check Service status for manatee-sitter on the provided vm/server pair.
 *
 * @param {Object} opts: required. The following options are mandatory:
 *      @param {String} vm: UUID of the manatee VM where we want to check
 *          manatee-sitter service status
 *      @param {String} server: UUID of server hosting vm
 *      @param {Object} log: bunyan loger instance
 * @param {Function} callback of the form f(err, svcStatus)
 */
function manateeSitterSvcStatus(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.server, 'opts.server');
    assert.string(opts.vm, 'opts.vm');
    assert.func(callback, 'callback');

    opts.log.trace({
        server: opts.server,
        zone: opts.vm
    }, 'Checking manatee sitter status (sdc-oneachnode)');

    var argv = [
        '/opt/smartdc/bin/sdc-oneachnode',
        '-j',
        '-n',
        opts.server,
        format('/usr/bin/svcs -z %s -o state -H manatee-sitter', opts.vm)
    ];

    common.execFilePlus({
        argv: argv,
        log: opts.log
    }, function (err, stdout, stderr) {
        if (err) {
            return callback(err);
        }

        try {
            // Due to the -j option of sdc-oneachnode:
            var res = JSON.parse(stdout);
            var out = res[0].result.stdout.trim() || null;
            return callback(null, out);
        } catch (e) {
            return callback(e);
        }
    });
}

/**
 * Wait for manatee given state
 *
 * @param {Object} opts required. The following options are mandatory:
 *      @param {String} manateeUUID: UUID of the manatee VM where we want to
 *          execute manatee-adm in order to check the current shard status
 *      @param {String} server: UUID of the server containing the aformentioned
 *          manatee VM
 *      @param {Object} log: Bunyan Logger instance
 *      @param state {String}: The desired manatee state. One of 'disabled' or
 *          'enabled'.
 *      @param role {String}: The role for the manatee shard member we want to
 *          check status. One of 'primary', 'sync' or 'async'
 * And the following are optional:
 *      @param {String} leaderIP: IP of the ZK leader to be used instead of ENV
 *      @param {Boolean} hasManatee21: when true, use `pg-status` instead
 *          of the deprecated `status`, which was added for version 2.1.0
 *          of manatee-adm
 * @param {Function} callback: of the form f(err).
 */
function waitForManatee(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.server, 'opts.server');
    assert.string(opts.manateeUUID, 'opts.manateeUUID');
    assert.string(opts.role, 'opts.role');
    assert.string(opts.state, 'opts.state');
    assert.optionalString(opts.leaderIP, 'opts.leaderIP');
    assert.optionalBool(opts.hasManatee21, 'opts.hasManatee21');
    assert.func(callback, 'callback');

    opts.hasManatee21 = opts.hasManatee21 || false;

    var func = (opts.hasManatee21) ? common.manateeShardStatus : getShardStatus;

    // Translate from deprecated shard status to simplified object format.
    // (see common.manateeShardStatus)
    function newStFormat(obj) {
        var shardPgSt = {};
        if (!obj.sdc) {
            return shardPgSt;
        }
        Object.keys(obj.sdc).forEach(function (m) {
            if ((m === 'primary' || m === 'sync') && !obj.sdc[m].error) {
                shardPgSt[m] = {
                    pg_status: obj.sdc[m].online ? 'ok': '-',
                    repl_status: (obj.sdc[m].repl &&
                        obj.sdc[m].repl.sync_state) ?
                            obj.sdc[m].repl.sync_state : '-'
                };
            } else if (!obj.sdc[m].error) {
                if (!shardPgSt[m]) {
                    shardPgSt[m] = [];
                }

                if (typeof (obj.sdc[m].online) !== 'undefined') {
                    shardPgSt[m].push({
                        pg_status: obj.sdc[m].online ? 'ok': '-'
                    });
                }
            }
        });
        opts.log.trace({
            original: obj.sdc,
            translated: shardPgSt
        }, 'Translated shard status');
        return shardPgSt;
    }

    if (opts.hasManatee21) {
        opts.vm = opts.manateeUUID;
    }

    var counter = 0;
    var limit = 180;
    function _waitForStatus() {
        func(opts, function (err, obj) {
            counter += 1;

            if (err) {
                return callback(err);
            }

            if (!opts.hasManatee21) {
                obj = newStFormat(obj);
            }

            var done = false;

            switch (opts.role) {
            case 'primary':
                if (opts.state === 'disabled') {
                    if (!obj.primary || obj.primary.pg_status !== 'ok') {
                        done = true;
                    }
                } else {
                    // Online as soon as pg-status reports it is online
                    if (obj.primary && obj.primary.pg_status === 'ok') {
                        done = true;
                    }
                }
                break;
            case 'sync':
                if (opts.state === 'disabled') {
                    if (!obj.sync || obj.sync.pg_status !== 'ok') {
                        done = true;
                    }
                } else {
                    // Enabled only if it's online and has caught up with
                    // primary replication
                    if (obj.sync && obj.sync.pg_status === 'ok' &&
                        obj.primary && obj.primary.repl_status === 'sync') {
                        done = true;
                    }
                }
                break;
            case 'async':
                if (opts.state === 'disabled') {
                    if (!obj.async || !obj.async.length ||
                            obj.async[0].pg_status !== 'ok') {
                        done = true;
                    }
                } else {
                    if (!obj.async || !obj.async.length) {
                        done = false;
                    } else {
                        if (obj.async[0].pg_status === 'ok' && obj.sync &&
                                obj.sync.repl_status === 'async') {
                            done = true;
                        }
                    }
                }
                break;
            default:
                return callback(new errors.UsageError(
                            'Unknown manatee role ' + opts.role));
            }


            opts.log.trace({
                shard: obj,
                done: done,
                state: opts.state,
                role: opts.role
            }, 'manatee shard state object');

            if (done) {
                return callback(null);
            }

            // If mode is deposed, it will not change nevermore, let's
            // return here and avoid waiting for anything else
            if (obj.deposed) {
                return callback('deposed');
            }

            if (counter < limit) {
                // Before queuing the next check, verify manatee-sitter
                // didn't went into maintenance on the machine we're trying
                // to get shard status:
                manateeSitterSvcStatus({
                    log: opts.log,
                    server: opts.server,
                    vm: opts.manateeUUID
                }, function (er2, st) {
                    if (er2) {
                        return callback(er2);
                    }
                    if (st === 'maintenance') {
                        return callback(new InternalError(new Error(
                            'manatee-sitter went into maintenance')));
                    }
                    return setTimeout(_waitForStatus, 5000);
                });
            } else {
                return callback(format(
                    'Timeout (15m) waiting for manatee %s to be %s',
                    opts.role, opts.state));
            }

        });
    }
    _waitForStatus();
}


// Not exported:
function _svcadmRemoteService(opts, callback) {
    opts.log.trace({
        server: opts.server,
        zone: opts.zone,
        fmri: opts.fmri
    }, format('%s remote service (sdc-oneachnode)', opts.cmd));
    var argv = [
        '/opt/smartdc/bin/sdc-oneachnode',
        '-n',
        opts.server
    ];
    // Only svcadm enable/disable take the '-s' option:
    if (opts.cmd !== 'enable' && opts.cmd !== 'disable') {
        argv.push(format('svcadm -z %s %s %s; ',
                    opts.zone, opts.cmd, opts.fmri));
    } else {
        argv.push(format('svcadm -z %s %s -s %s; ',
                    opts.zone, opts.cmd, opts.fmri));
    }
    common.execFilePlus({
        argv: argv,
        log: opts.log
    }, function (err, stdout, stderr) {
        if (err) {
            callback(err);
        } else {
            callback(null, stdout, stderr);
        }
    });
}

function restartRemoteSvc(opts, callback) {
    opts.cmd = 'restart';
    _svcadmRemoteService(opts, callback);
}

function disableRemoteSvc(opts, callback) {
    opts.cmd = 'disable';
    _svcadmRemoteService(opts, callback);
}

function enableRemoteSvc(opts, callback) {
    opts.cmd = 'enable';
    _svcadmRemoteService(opts, callback);
}

// Disable manatee-sitter service across all the manatees on the SDC cluster
function disableManateeSitter(opts, cb) {
    var shard = opts.shard;
    if (shard.sdc) {
        shard = shard.sdc;
    }
    var log = opts.log;
    var progress = opts.progress;
    var leaderIP = opts.leaderIP || null;
    var hasManatee21 = opts.hasManatee21 || false;

    vasync.pipeline({funcs: [
        function disableAsyncManatee(_, next) {
            if (!shard.async || !shard.async.length) {
                return next();
            }
            progress('Disabling async manatee');
            disableRemoteSvc({
                server: shard.async[0].server,
                zone: shard.async[0].zoneId,
                fmri: 'manatee-sitter',
                log: log
            }, next);
        },

        function waitAsyncDisabled(_, next) {
            if (!shard.async || !shard.async.length) {
                return next();
            }
            progress('Waiting for async manatee to be disabled');
            var _opts = {
                state: 'disabled',
                role: 'async',
                server: shard.primary.server,
                manateeUUID: shard.primary.zoneId,
                log: log,
                hasManatee21: hasManatee21
            };
            if (leaderIP) {
                _opts.leaderIP = leaderIP;
            }
            waitForManatee(_opts, next);
        },

        function disableSyncManatee(_, next) {
            if (!shard.sync) {
                return next();
            }
            progress('Disabling sync manatee');
            disableRemoteSvc({
                server: shard.sync.server,
                zone: shard.sync.zoneId,
                fmri: 'manatee-sitter',
                log: log
            }, next);
        },

        function waitSyncDisabled(_, next) {
            if (!shard.sync) {
                return next();
            }
            progress('Waiting for sync manatee to be disabled');
            var _opts = {
                role: 'sync',
                state: 'disabled',
                server: shard.primary.server,
                manateeUUID: shard.primary.zoneId,
                log: log,
                hasManatee21: hasManatee21
            };
            if (leaderIP) {
                _opts.leaderIP = leaderIP;
            }
            waitForManatee(_opts, next);
        },

        function disablePrimaryManatee(_, next) {
            progress('Disabling primary manatee');
            disableRemoteSvc({
                server: shard.primary.server,
                zone: shard.primary.zoneId,
                fmri: 'manatee-sitter',
                log: log
            }, next);
        },

        function waitPrimaryDisabled(_, next) {
            progress('Waiting for primary manatee to be disabled');
            var _opts = {
                state: 'disabled',
                role: 'primary',
                server: shard.primary.server,
                manateeUUID: shard.primary.zoneId,
                log: log,
                hasManatee21: hasManatee21
            };
            if (leaderIP) {
                _opts.leaderIP = leaderIP;
            }
            waitForManatee(_opts, next);
        }
    ]}, cb);

}


// Enable manatee-sitter service across all the manatees on the SDC cluster.
// (See disableManateeSitter)
function enableManateeSitter(opts, cb) {
    var shard = opts.shard;
    if (shard.sdc) {
        shard = shard.sdc;
    }
    var log = opts.log;
    var progress = opts.progress;
    var leaderIP = opts.leaderIP || null;
    var hasManatee21 = opts.hasManatee21 || false;

    vasync.pipeline({funcs: [
        function enablePrimaryManatee(_, next) {
            progress('Enabling primary manatee');
            enableRemoteSvc({
                server: shard.primary.server,
                zone: shard.primary.zoneId,
                fmri: 'manatee-sitter',
                log: log
            }, next);

        },

        function waitPrimaryEnabled(_, next) {
            progress('Waiting for primary manatee to be enabled');
            var _opts = {
                state: 'enabled',
                role: 'primary',
                server: shard.primary.server,
                manateeUUID: shard.primary.zoneId,
                log: log,
                hasManatee21: hasManatee21
            };
            if (leaderIP) {
                _opts.leaderIP = leaderIP;
            }
            waitForManatee(_opts, next);
        },

        function enableSyncManatee(_, next) {
            if (!shard.sync) {
                return next();
            }

            progress('Enabling sync manatee');
            enableRemoteSvc({
                server: shard.sync.server,
                zone: shard.sync.zoneId,
                fmri: 'manatee-sitter',
                log: log
            }, next);
        },

        function waitSyncEnabled(_, next) {
            if (!shard.sync) {
                return next();
            }
            progress('Waiting for sync manatee to be enabled');
            var _opts = {
                role: 'sync',
                state: 'enabled',
                server: shard.primary.server,
                manateeUUID: shard.primary.zoneId,
                log: log,
                hasManatee21: hasManatee21
            };
            if (leaderIP) {
                _opts.leaderIP = leaderIP;
            }
            waitForManatee(_opts, next);
        },

        function enableAsyncManatee(_, next) {
            if (!shard.async || !shard.async.length) {
                return next();
            }
            progress('Enabling async manatee');
            enableRemoteSvc({
                server: shard.async[0].server,
                zone: shard.async[0].zoneId,
                fmri: 'manatee-sitter',
                log: log
            }, next);
        },

        function waitAsyncEnabled(_, next) {
            if (!shard.async || !shard.async.length) {
                return next();
            }
            progress('Waiting for async manatee to be enabled');
            var _opts = {
                role: 'async',
                state: 'enabled',
                server: shard.primary.server,
                manateeUUID: shard.primary.zoneId,
                log: log,
                hasManatee21: hasManatee21
            };
            if (leaderIP) {
                _opts.leaderIP = leaderIP;
            }
            waitForManatee(_opts, next);
        }

    ]}, cb);
}

/**
 * Wait until all the members of the given ZK cluster return 'imok' or
 * timeout after 5 minutes (experienced based value, may want to make it
 * configurable too):
 *
 * @param {Object} opts:
 *          - log {Bunyan Logger}
 *          - ips {Array}: the list of IPs for each one of the ZK cluster
 *          members.
 * @param {Function} callback: of the form f(err).
 */
function wait4ZkOk(opts, callback) {
    var counter = 0;
    var limit = 60;
    function _wait4Zk() {
        vasync.forEachParallel({
            inputs: opts.ips,
            func: function zkInstStatus(ip, next_) {
                var c = format('echo ruok | nc %s 2181; echo ""', ip);
                common.execPlus({
                    cmd: c,
                    log: opts.log
                }, function (err, stdout, stderr) {
                    if (err) {
                        next_(err);
                    } else {
                        next_(null, stdout.trim());
                    }
                });
            }
        }, function (waitErr, results) {
            if (waitErr) {
                return callback(waitErr);
            }
            counter += 1;
            var notOk = results.successes.filter(function (r) {
                return (r !== 'imok');
            });

            if (notOk.length) {
                if (counter < limit) {
                    return setTimeout(_wait4Zk, 5000);
                } else {
                    return callback('Timeout (5min) waiting ' +
                            'for ZK cluster');
                }
            }

            return callback();
        });
    }

    _wait4Zk();
}


/**
 * Wait until all the members of the given ZK cluster have joined the cluster
 * either as 'leader' or 'follower', or timeout after 5 minutes (experienced
 * based value, may want to make it configurable too):
 *
 * @param {Object} opts:
 *          - log {Bunyan Logger}
 *          - ips {Array}: the list of IPs for each one of the ZK cluster
 *          members.
 * @param {Function} callback: of the form f(err).
 */
function wait4ZkCluster(opts, callback) {
    var counter = 0;
    var limit = 60;
    function _wait4ZkMode() {
        vasync.forEachParallel({
            inputs: opts.ips,
            func: function zkInstStatus(ip, next_) {
                var c = format(
                    'echo stat | nc %s 2181 | grep -i "mode"', ip);
                common.execPlus({
                    cmd: c,
                    log: opts.log
                }, function (err, stdout, stderr) {
                    if (err) {
                        // The command throws an error while ZK is
                        // transitioning from standalone to cluster member
                        next_(null, 'transitioning');
                    } else {
                        next_(null,
                            stdout.trim().replace(/^Mode:\s/, ''));
                    }
                });
            }
        }, function (waitErr, results) {
            if (waitErr) {
                return callback(waitErr);
            }
            counter += 1;
            var notOk = results.successes.filter(function (r) {
                return (r !== 'leader' && r !== 'follower');
            });

            if (notOk.length && counter < limit) {
                if (counter < limit) {
                    return setTimeout(_wait4ZkMode, 5000);
                } else {
                    return callback('Timeout (5min) waiting ' +
                            'for ZK cluster');
                }
            }
            return callback();
        });
    }

    _wait4ZkMode();
}

/**
 * Get the IP for the ZK leader instance.
 * @param {Object} opts:
 *          - log {Bunyan Logger}
 *          - ips {Array}: the list of IPs for each one of the ZK cluster
 *          members.
 * @param {Function} callback: of the form f(err, ip).
 */

function getZkLeaderIP(opts, callback) {
    vasync.forEachParallel({
        inputs: opts.ips,
        func: function zkInstStatus(ip, next_) {
            var c = format(
                'echo stat | nc %s 2181 | grep -i "mode"', ip);
            common.execPlus({
                cmd: c,
                log: opts.log
            }, function (err, stdout, stderr) {
                if (err) {
                    // The command throws an error while ZK is
                    // transitioning from standalone to cluster member
                    next_(null, {ip: ip, mode: 'transitioning'});
                } else {
                    next_(null, {
                        ip: ip,
                        mode: stdout.trim().replace(/^Mode:\s/, '')
                    });
                }
            });
        }
    }, function (waitErr, results) {
        if (waitErr) {
            return callback(waitErr);
        }

        var leader = results.successes.filter(function (r) {
            return (r.mode === 'leader');
        });

        var IP = leader.length ? leader[0].ip : null;
        return callback(null, IP);
    });

}

/**
 * Get shard (zk-)state using manatee-adm.
 *
 * @param {Object} opts:
 *
 * All the following options are required:
 *
 *      @param {String} manateeUUID: UUID of the manatee VM we want to run
 *          manatee-adm into
 *      @param {String} server: server UUID for aforementioned manatee VM
 *      @param {Object} log: bunyan logger instance
 *
 * The following options are optional:
 *
 *      @param {String} leaderIP: IP address of the Zookeeper cluster to
 *          use for state retrieval
 *      @param {Boolean} hasManatee21: when true, use `zk-state` instead
 *          of the deprecated `state`, which was added for version 2.1.0
 *          of manatee-adm
 *
 * @param {Function} callback: Callback of the form f(err, zkShardState)
 */
function getShardState(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.server, 'opts.server');
    assert.string(opts.manateeUUID, 'opts.manateeUUID');
    assert.optionalString(opts.leaderIP, 'opts.leaderIP');
    assert.optionalBool(opts.hasManatee21, 'opts.hasManatee21');
    assert.func(callback, 'callback');

    opts.hasManatee21 = opts.hasManatee21 || false;

    var cmd = (opts.hasManatee21) ? 'zk-state' : 'state';

    if (opts.leaderIP) {
        cmd += ' -z ' + opts.leaderIP + ':2181';
    }

    common.manateeAdmRemote({
        server: opts.server,
        vm: opts.manateeUUID,
        cmd: cmd,
        log: opts.log
    }, function (err, res, stderr) {
        if (err) {
            return callback(err);
        }

        var manateeShard = JSON.parse(res);
        return callback(null, manateeShard);
    });
}

/**
 * Wait for all the moray services into all the given moray instances to be up
 * and running without errors.
 *
 * @param {Object} opts: All the following options are required:
 *      @param {Object} opts.vms: an array of moray Vms; each of them must
 *          have the following properties:
 *              @param {String} uuid: vm UUID
 *              @param {String} server_uuid: vm's server UUID
 * @param {Object} opts.sdcadm: sdcadm object instance
 *
 * @param {Function} cb: Callback of the form f(err, stdout, stderr);
 */
function wait4Morays(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.vms, 'opts.vms');
    assert.object(opts.sdcadm, 'opts.sdcadm');
    assert.func(cb, 'cb');

    var log = opts.sdcadm.log;

    function waitForMoray(inst, next_) {
        var counter = 0;
        var limit = 120;
        function _wait4Moray() {
            opts.sdcadm.checkHealth({
                insts: [inst]
            }, function (err, results) {
                if (err) {
                    log.debug({
                        err: err
                    }, 'checkInstSvcs');
                    return next_(err);
                }
                counter += 1;
                log.debug({
                    results: results
                }, 'checkInstSvcs');
                var res = results[0];
                if (res.health_errors && res.health_errors.length) {
                    if (counter < limit) {
                        return setTimeout(_wait4Moray, 5000);
                    } else {
                        return next_(new errors.InstanceIsDownError(
                                res.health_errors[0].message));
                    }
                } else {
                    return next_();
                }
            });
        }
        _wait4Moray();
    }

    vasync.forEachParallel({
        inputs: opts.vms,
        func: function wait4MoraySvcs(vm, next_) {
            waitForMoray({
                instance: vm.uuid,
                zonename: vm.uuid,
                uuid: vm.uuid,
                server: vm.server_uuid,
                service: 'moray',
                type: 'vm'
            }, next_);
        }
    }, function (pError) {
        if (pError) {
            return cb(pError);
        }
        return cb();
    });
}

// --- exports

module.exports = {
    getUserScript: getUserScript,
    writeOldUserScriptForRollback: writeOldUserScriptForRollback,
    updateSvcUserScript: updateSvcUserScript,
    updateVmUserScript: updateVmUserScript,
    getOldUserScript: getOldUserScript,
    updateSapiSvc: updateSapiSvc,
    imgadmInstall: imgadmInstall,
    reprovision: reprovision,
    waitForInstToBeUp: waitForInstToBeUp,
    checkHA: checkHA,
    provisionTmpVm: provisionTmpVm,
    waitForTmpInstToBeUp: waitForTmpInstToBeUp,
    getTmpInstanceUUID: getTmpInstanceUUID,
    checkIfTmpVMHasErrors: checkIfTmpVMHasErrors,
    disableVMRegistrar: disableVMRegistrar,
    waitUntilVMNotInDNS: waitUntilVMNotInDNS,
    waitUntilVmInDNS: waitUntilVmInDNS,
    stopTmpVm: stopTmpVm,
    destroyTmpVM: destroyTmpVM,
    createInstance: createInstance,
    imgadmInstallRemote: imgadmInstallRemote,
    reprovisionRemote: reprovisionRemote,
    disableVMRegistrarRemote: disableVMRegistrarRemote,
    updateVmUserScriptRemote: updateVmUserScriptRemote,
    ensureDelegateDataset: ensureDelegateDataset,
    disableManateeSitter: disableManateeSitter,
    enableManateeSitter: enableManateeSitter,
    getShardStatus: getShardStatus,
    restartRemoteSvc: restartRemoteSvc,
    disableRemoteSvc: disableRemoteSvc,
    enableRemoteSvc: enableRemoteSvc,
    wait4ZkOk: wait4ZkOk,
    wait4ZkCluster: wait4ZkCluster,
    getZkLeaderIP: getZkLeaderIP,
    getShardState: getShardState,
    wait4Morays: wait4Morays,
    manateeSitterSvcStatus: manateeSitterSvcStatus,
    waitForManatee: waitForManatee
};
// vim: set softtabstop=4 shiftwidth=4:
