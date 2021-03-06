<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

Here-in random TODOs and scratchpad notes for sdcadm.

# now

- add 'login' ability to each zone (a la manta-login) ... could use that
  for 'up-check' for each service


# TODO

- ... sync this file with the roadmap

- basic sdcadm test suite

- sdcadm update -a|--all
  sdcadm update -f update-spec.json

- sdcadm history (and having updates adding to this history file)
  /var/sdcadm/history   # rotate this? -> moray

- extra procedure on update: `sdc-amonadm update` (library equivalent) procedure
  after instance changes

- Allow no-op updates (same image) and downgrades with a "force" flag
  Want the confirmation to break out instances meaningfully, e.g.:

        download 1 image (69 MiB):
            image 6261c204-e75d-11e3-91fa-a311fd4ab601 (ca@master-20140529T180636Z-gf4e65ef)
        update "ca" service (1 instance) to image 6261c204-e75d-11e3-91fa-a311fd4ab601 (ca@master-20140529T180636Z-gf4e65ef)

        update "ca" service (2 instances) to image 6261c204-e75d-11e3-91fa-a311fd4ab601 (ca@master-20140529T180636Z-gf4e65ef)

        update "vm-agent" service (300 instances) to image 6261c204-e75d-11e3-91fa-a311fd4ab601 (vm-agent@1.2.3):
            289 instances will be updated from image $oldImageUuid1
            1 instance will be updated from image $oldImageUuid2
            10 instances already at image $imageUuid

        update "vm-agent" service (300 instances) to image 6261c204-e75d-11e3-91fa-a311fd4ab601 (vm-agent@1.2.3):
            300 instances forced downgrade from image $oldImageUuid1

  What does that break-out example look like in code path? That
  needs to be in the result of 'determineProcedures'. A -F will
  translate to a `'allowNoopOrDowngrade':true` or similar, which
  'determineProcedures' will handle.

- a way to list available updates (with changelog support)
  Perhaps use this under the hood:
        update-imgadm changes $from-image-uuid $to-image-uuid
  Ideas:
        sdcadm avail              # show all avail images for all services
  Should that just show the latest? Let's run with that. ^^^ is just a list
  of latest image for each service then.
        sdcadm avail dapi imgapi  # for just that service(s)

  List the changes for these images compared to the current earliest (e.g.
  to the earlier version if have two images for teh same service in play)
        sdcadm avail -c|--changes

  If you have different images for, say, dapi0 and dapi1, then might want
  changes relative to a particular one:
        sdcadm avail -c dapi0    # i.e. can use ALIAS there (or UUID)

  Getting particular change logs:
        sdcadm changes dapi0 dapi1  # list changes between two current dapi's using diff images
        sdcadm changes $imageUuidFrom $imageUuidTo

- conformance tests as *part of sdcadm*? Then sdcadm can run them (or a fast
  subset) as a sanity check after each update perhaps.

- trim out stuff in node_modules in the sdcadm shar (e.g. large ldapjs
  and restify bits that we don't need)

- for migration: `sdcadm svcs` listing:
        metadata.sdcMigratedRev,
        collected current max sdcMigrationRev of deployed instance images


- perhaps 'sdcadm setup --ha' for help setting up recommended numbers of
  extra instances. Dunno.
  Want some command(s) to help with the post-headnode-setup steps to make this
  thing real. Adding cloudapi, external nics, etc.

# Design Qs

- Currently the usage of IMGAPI AdminImportRemoteImage to import update images
  requires the imgapi zone to have an external NIC. Do we want to require that?
  The DownloadImages procedure should have an option for this.

# SAPI + agents notes

From discussion with jclulow.

- new ur: spec'd HTTP websocket API to go into base smartos, backfill
  installer for unupgraded platforms, move to talking to it
- CN setup moves to installing a sdc-agent that is the controller for
  handling update/state/drain, etc. of all the other agents.
  (note: "agent" here isn't necessarily just running services, but any
  blob of software that needs to be installed on a node.)
- sdc-agent heartbeats into CNAPI with agent data (a la VMs in VMAPI)
  Then CNAPI grows a ListAgents, GetAgent, etc.
- /instances from the real VMs, *including* instance.metadata being
  vm.customer_metadata
- add services to SAPI for the agents, and SAPI GetInstances includes
  agent info from CNAPI's ListAgents (for now from ServerList sysinfo)

    SAPI agent instances:
        GET /instances/$server_uuid/$service_name
    e.g.:
        GET /instances/c5ecfd70-e496-2c46-a18b-b295c9d2644e/provisioner

- for consistency have instance-level metadata for agents, where the authority
  for that data is the moray info in CNAPI for that agent.
- config-agent in GZ, whether a subset of the sdc-agent or separate


# self-update with upgrade of other components

This isn't currently supported in the first pass. Some thoughts
on how this could work.

    # $SDCADM_UPGRADE_DIR/upgrade.json was written out

    set -e
    cd /var/tmp/sdcadm-$timestamp
    sh ./sdcadm-install.sh

    # Note: SDCADM_UPGRADE_TIMESTAMP envvar is the signal to sdcadm upgrade to
    # use the current inprogress upgrade.
    SDCADM_UPGRADE_TIMESTAMP=... exec sdcadm upgrade


# do we need to support no external access from GZ?

Do we need to route all external traffic via the 'sdc' zone? Currently
we are assuming can reach out from the GZ where 'sdcadm' is running.



# node vs server vs hostname vs host

We are inconsistent: sdc-server, CNAPI, /servers, 'cn' and 'host' in manta-adm,
'server' in sdcadm, 'node' in sdc-oneachnode.
'server_uuid' in VMAPI, vmadm, SAPI, etc.

-n: sdc-oneachnode
-H HOSTNAME: sdcadm
-s SERVER: sdcadm



# sdcadm cn

A la manta-adm cn: https://mail.google.com/mail/u/1/?ui=2#inbox/1456beddc1c484a0

Also s/server/cn/ in terminology elsewhere. Hrm, it seems to be "cn" or "server_uuid". Not sure.
Also s/hostname/host/ apparently in manta-adm. Or just have it as a shortcut name?


# image versioning

- go another round on including 'BRANCH/' prefix with dap?
...


# hard to handle upgrade issues

TODO: figure out how to deal with these later

Q: What does manta-adm do for that kind of thing? For config requirement
changes during upgrade. E.g. a new muskie requires that its SAPI service
have metadata.MUSKIE_FOO? Just don't do that?
E.g. adding a config var like "region_name".
E.g. changing the spec for memcaps for zones
E.g. cleaning out sdcpackage entries from UFDS
E.g. migrations (e.g. imgapi migrations)


# add 'sdc-foundation' service/package

For changes to the service data themselves we need a image/version of
sdc-foundation (or whatever). Perhaps that could be the 'sdc' zone. It
would map well. If not, then we'd need a separate "instance" to capture
that config version.

Which is just config changes, invariants. It holds the sapi configs (or the
code to calculate them).



# HN setup, CN setup

Say headnode.sh calls 'sdcadm setup' for most of the work (at least the
installation of agents and core zone creation). After setup you have
SAPI setup in full mode with /instances that match reality.

When setting up a CN, what about agents? Currently they just pull the shar
from assets zone IP (given in boot params). I don't know that we'd want
control for installing agents on a new CN to come from a central authority,
do we? If not, then either want /instances (for agents at least) to be
reflection of reality rather than explicitly added entries. **Let's run with
/instances being a reflection of reality (i.e. what 'sdcadm insts' does now).**

Ideally I'd like CN boot to be given an imgapi DNS (perhaps inferred from
node.config) and a set of agent UUIDs to download and install, if that is
possible. Then we don't need the shar. Agents "images" would be put in
/usbkey/datasets [sic] and loaded into imgapi along with the others.

