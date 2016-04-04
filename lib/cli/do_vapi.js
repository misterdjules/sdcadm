/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * The 'sdcadm experimental vapi' CLI subcommand.
 */

var util = require('util'),
    format = util.format;
var vasync = require('vasync');

var common = require('../common');
var errors = require('../errors');
var DownloadImages = require('../procedures/download-images').DownloadImages;
var shared = require('../procedures/shared');
var steps = require('../steps');


function do_vapi(subcmd, opts, args, cb) {
    var self = this;
    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    } else if (args.length > 0) {
        return cb(new errors.UsageError('too many args: ' + args));
    }

    var start = Date.now();
    var svcData = {
        name: 'vapi',
        params: {
            package_name: 'sdc_1024',
            billing_id: 'TO_FILL_IN', // filled in from 'package_name'
            image_uuid: 'TO_FILL_IN',
            archive_on_delete: true,
            delegate_dataset: true,
            maintain_resolvers: true,
            networks: [
                {name: 'admin'}
            ],
            firewall_enabled: false,
            tags: {
                smartdc_role: 'vapi',
                smartdc_type: 'core'
            }
        },
        metadata: {
            SERVICE_NAME: 'vapi',
            SERVICE_DOMAIN: 'TO_FILL_IN',
            'user-script': 'TO_FILL_IN'
        }
    };


    var context = {
        imgsToDownload: [],
        didSomething: false
    };
    vasync.pipeline({arg: context, funcs: [
        function getPkg(ctx, next) {
            console.log('getPkg');
            var filter = {name: svcData.params.package_name,
                active: true};
            self.sdcadm.papi.list(filter, {}, function (err, pkgs) {
                if (err) {
                    return next(err);
                } else if (pkgs.length !== 1) {
                    return next(new errors.InternalError({
                        message: format('%d "%s" packages found', pkgs.length,
                            svcData.params.package_name)
                    }));
                }
                ctx.vapiPkg = pkgs[0];
                next();
            });
        },

        function ensureSapiMode(_, next) {
            console.log('ensureSapiMode');
            // Bail if SAPI not in 'full' mode.
            self.sdcadm.sapi.getMode(function (err, mode) {
                if (err) {
                    next(new errors.SDCClientError(err, 'sapi'));
                } else if (mode !== 'full') {
                    next(new errors.UpdateError(format(
                        'SAPI is not in "full" mode: mode=%s', mode)));
                } else {
                    next();
                }
            });
        },

        function getSvc(ctx, next) {
            console.log('getSvc');
            self.sdcadm.sapi.listServices({
                name: 'vapi',
                application_uuid: self.sdcadm.sdc.uuid
            }, function (svcErr, svcs) {
                if (svcErr) {
                    return next(svcErr);
                } else if (svcs.length) {
                    ctx.vapiSvc = svcs[0];
                }
                next();
            });
        },

        function getVapiInst(ctx, next) {
            console.log('getVapiInst');
            if (!ctx.vapiSvc) {
                return next();
            }
            var filter = {
                service_uuid: ctx.vapiSvc.uuid
            };
            self.sdcadm.sapi.listInstances(filter, function (err, insts) {
                if (err) {
                    return next(new errors.SDCClientError(err, 'sapi'));
                } else if (insts && insts.length) {
                    // Note this doesn't handle multiple insts.
                    ctx.vapiInst = insts[0];
                    self.sdcadm.vmapi.getVm({uuid: ctx.vapiInst.uuid},
                            function (vmErr, vapiVm) {
                        if (vmErr) {
                            return next(vmErr);
                        }
                        ctx.vapiVm = vapiVm;
                        next();
                    });
                } else {
                    next();
                }
            });
        },

        function getLatestVapiImage(ctx, next) {
            console.log('getLatestVapiImage');
            var filter = {name: 'vapi'};
            self.sdcadm.updates.listImages(filter, function (err, images) {
                if (err) {
                    next(err);
                } else if (images && images.length) {
                    // TODO presuming sorted
                    ctx.vapiImg = images[images.length - 1];
                    next();
                } else {
                    next(new errors.UpdateError('no "vapi" image found'));
                }
            });
        },

        function haveVapiImageAlready(ctx, next) {
            console.log('haveVapiImageAlready');
            self.sdcadm.imgapi.getImage(ctx.vapiImg.uuid,
                    function (err, img_) {
                if (err && err.body && err.body.code === 'ResourceNotFound') {
                    ctx.imgsToDownload.push(ctx.vapiImg);
                } else if (err) {
                    return next(err);
                }
                next();
            });
        },

        function importImages(ctx, next) {
            console.log('importImages');
            if (ctx.imgsToDownload.length === 0) {
                return next();
            }
            var proc = new DownloadImages({
                images: ctx.imgsToDownload,
                source: opts['img-source']
            });
            proc.execute({
                sdcadm: self.sdcadm,
                log: self.log,
                progress: self.progress
            }, next);
        },

        /* @field ctx.userString */
        shared.getUserScript,

        function createVapiSvc(ctx, next) {
            console.log('createVapiSvc');
            if (ctx.vapiSvc) {
                return next();
            }

            var domain = self.sdcadm.sdc.metadata.datacenter_name + '.' +
                    self.sdcadm.sdc.metadata.dns_domain;
            var svcDomain = svcData.name + '.' + domain;

            self.progress('Creating "vapi" service');
            ctx.didSomething = true;
            svcData.params.image_uuid = ctx.vapiImg.uuid;
            svcData.metadata['user-script'] = ctx.userScript;
            svcData.metadata['SERVICE_DOMAIN'] = svcDomain;
            svcData.params.billing_id = ctx.vapiPkg.uuid;
            delete svcData.params.package_name;

            self.sdcadm.sapi.createService('vapi', self.sdcadm.sdc.uuid,
                    svcData, function (err, svc) {
                if (err) {
                    return next(new errors.SDCClientError(err, 'sapi'));
                }
                ctx.vapiSvc = svc;
                self.log.info({svc: svc}, 'created vapi svc');
                next();
            });
        },

        /* @field ctx.headnode */
        function getHeadnode(ctx, next) {
            console.log('getHeadnode');
            self.sdcadm.cnapi.listServers({
                headnode: true
            }, function (err, servers) {
                if (err) {
                    return next(new errors.SDCClientError(err, 'cnapi'));
                }
                ctx.headnode = servers[0];
                return next();
            });
        },
        function createVapiInst(ctx, next) {
            console.log('createVapiInst');
            if (ctx.vapiInst) {
                return next();
            }
            self.progress('Creating "vapi" instance');
            ctx.didSomething = true;
            var instOpts = {
                params: {
                    alias: 'vapi0',
                    server_uuid: ctx.headnode.uuid
                }
            };
            self.sdcadm.sapi.createInstance(ctx.vapiSvc.uuid, instOpts,
                    function (err, inst) {
                if (err) {
                    return next(new errors.SDCClientError(err, 'sapi'));
                }
                self.progress('Created VM %s (%s)', inst.uuid,
                    inst.params.alias);
                ctx.newVapiInst = inst;
                next();
            });
        },

        function done(ctx, next) {
            if (ctx.didSomething) {
                self.progress('Setup "vapi" (%ds)',
                    Math.floor((Date.now() - start) / 1000));
            }
            next();
        }
    ]}, cb);
}

do_vapi.options = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Show this help.'
    },
    {
        names: ['img-source', 's'],
        type: 'string',
        help: 'The URL of the images repository from which to download '
            + 'vapi\'s image'
    }
];
do_vapi.help = (
    'Create the "vapi" service and a first instance.\n' +
    '\n' +
    'Usage:\n' +
    '     {{name}} vapi\n' +
    '\n' +
    '{{options}}'
);

// --- exports

module.exports = {
    do_vapi: do_vapi
};
