#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2021  Yogesh Rajashekharaiah
# All Rights Reserved

import os
import sys
import argparse
import glob
from datetime import datetime
import xml.dom.minidom

# For pretty formatting on file
_HLEN = 10
_KLEN = 48
_VLEN = 34
_PSEP = '-'

def get_conf(xfl):
    ''' Extracts the text for the tags name and value under all property tag '''
    dct = {}
    doc = xml.dom.minidom.parse(xfl)
    props = doc.getElementsByTagName("property")

    for prop in props:
        name = prop.getElementsByTagName("name")[0].firstChild
        val = prop.getElementsByTagName("value")[0].firstChild
        try:
            dct[name.data] = val.data
        except AttributeError:
            # Sometimes value can be null
            print("File: %s with property: %s has empty value" %(xfl, name.data))
    return dct

def compare_clust(args, resdir, flist):
    ''' Loops through all files common between Left and Right cluster and outputs
        the missing properties and differences in the resultdir folder
    '''
    # Properties with sensitive values or values that will always be different between the clusters
    xml_props_ign = {'dfs.nameservices',
                     'dfs.namenode.shared.edits.dir',
                     'fs.defaultFS',
                     'dfs.namenode.servicerpc-address',
                     'dfs.namenode.http-address',
                     'dfs.https.address',
                     'mapreduce.jobhistory.address',
                     'mapreduce.jobhistory.webapp.https.address',
                     'mapreduce.jobhistory.webapp.address',
                     'yarn.resourcemanager.webapp.https.address',
                     'yarn.resourcemanager.resource-tracker.address',
                     'yarn.resourcemanager.admin.address',
                     'yarn.resourcemanager.address',
                     'yarn.resourcemanager.zk-address',
                     'yarn.resourcemanager.webapp.address',
                     'yarn.resourcemanager.scheduler.address',
                     'hadoop.security.key.provider.path',
                     'ssl.client.truststore.password',
                     'hive.zookeeper.ssl.keystore.password',
                     'hive.zookeeper.ssl.truststore.password',
                     'hive.server2.webui.keystore.password',
                     'hive.server2.keystore.password',
                     'hive.zookeeper.ssl.keystore.password',
                     'hive.zookeeper.ssl.truststore.password',
                     'ssl.server.keystore.password',
                     'ssl.server.keystore.keypassword',
                     'ranger.plugin.hbase.trusted.proxy.ipaddress',
                     'ranger.plugin.hdfs.trusted.proxy.ipaddress',
                     'ranger.plugin.hive.trusted.proxy.ipaddress',
                     'ranger.plugin.yarn.trusted.proxy.ipaddress',
                     'hbase.rootdir',
                     'hbase.zookeeper.quorum',
                     'hive.zookeeper.quorum',
                     'hadoop.registry.zk.quorum',
                     'javax.jdo.option.ConnectionURL',
                     'ranger.plugin.hdfs.policy.rest.url',
                     'ranger.plugin.hive.policy.rest.url',
                     'ranger.plugin.yarn.policy.rest.url',
                     'ranger.plugin.atlas.policy.rest.url',
                     'ranger.plugin.hbase.policy.rest.url',
                     'ranger.plugin.hdfs.policy.rest.url',
                     'ranger.plugin.hive.policy.rest.url',
                     'ranger.plugin.yarn.policy.rest.url',
                     'ranger.plugin.atlas.policy.rest.url',
                     'ranger.plugin.hbase.policy.rest.url',
                     'ranger.plugin.kafka.policy.rest.url',
                     'ranger.plugin.knox.policy.rest.url',
                     'ranger.plugin.kudu.policy.rest.url',
                     'ranger.plugin.kms.policy.rest.url',
                     'ranger.plugin.schema-registry.policy.rest.url',
                     'ranger.plugin.kafka.policy.rest.url',
                     'xasecure.policymgr.clientssl.truststore.password',
                     'xasecure.policymgr.clientssl.keystore.password',
                     'ranger.plugin.kafka.access.cluster.name',
                     'ranger.plugin.knox.access.cluster.name',
                     'ranger.plugin.hive.access.cluster.name',
                     'ranger.plugin.kudu.access.cluster.name',
                     'ranger.plugin.kms.access.cluster.name',
                     'ranger.plugin.schema-registry.access.cluster.name',
                     'ranger.plugin.kafka.access.cluster.name',
                     'ranger.plugin.atlas.access.cluster.name',
                     'ranger.plugin.hdfs.access.cluster.name',
                     'ranger.plugin.hbase.access.cluster.name',
                     'ranger.plugin.yarn.access.cluster.name',
                     'oozie.service.JPAService.jdbc.url',
                     'oozie.service.JPAService.jdbc.username',
                     'oozie.service.JPAService.jdbc.password',
                     'oozie.actions.default.name-node',
                     'oozie.service.HadoopAccessorService.nameNode.whitelist',
                     'oozie.service.HadoopAccessorService.jobTracker.whitelist',
                     'oozie.base.url',
                     'oozie.service.CallbackService.base.url',
                     'phoenix.queryserver.tls.truststore.password',
                     'ranger.jpa.jdbc.password',
                     'ranger.jpa.jdbc.user',
                     'ranger.jpa.jdbc.url',
                     'ranger.truststore.password',
                     'ranger.keystore.password',
                     'ranger.usersync.truststore.password',
                     'ranger.usersync.keystore.password',
                     'ranger.db.encrypt.key.password',
                     'ranger.ks.jpa.jdbc.password',
                     'ranger.ks.keystore.password',
                     'ranger.ks.truststore.password',
                     'ranger.service.https.attrib.keystore.pass'
                     'xasecure.audit.destination.hdfs.dir',
                     'xasecure.audit.destination.solr.zookeepers'}
    for rfl in flist:
        print("\n" + "Processing file %s" %rfl)
        lclstr = get_conf(os.path.join(args.left, rfl))
        rclstr = get_conf(os.path.join(args.right, rfl))
        rlst = []

        # Remove properties from ignore list before comparison
        l_s = set(lclstr.keys()) - xml_props_ign
        r_s = set(rclstr.keys()) - xml_props_ign
        lmiss = r_s - l_s
        rmiss = l_s - r_s

        # Print only if missingprops=y (default)
        if args.missingprops == 'y':
            if lmiss:
                rlst.append("*"*_HLEN + "Properties missing in Left Cluster" + "*"*_HLEN)
                rlst.append("\nProperty".ljust(_KLEN) + "   " + "Value".ljust(_VLEN))
                rlst.append("\n" + _PSEP*_KLEN + "  " + _PSEP*_VLEN + "\n")
                for k in lmiss:
                    rlst.append(k.ljust(_KLEN) + "  " + rclstr[k].ljust(_VLEN) + "\n")

            if rmiss:
                if lmiss:
                    rlst.append("\n"*2 + "*"*_HLEN + "Properties missing in Right Cluster" + "*"*_HLEN)
                else:
                    rlst.append("*"*_HLEN + "Properties missing in Right Cluster" + "*"*_HLEN)
                rlst.append("\nProperty".ljust(_KLEN) + "   " + "Value".ljust(_VLEN))
                rlst.append("\n" + _PSEP*_KLEN + "  " + _PSEP*_VLEN + "\n")
                for k in rmiss:
                    rlst.append(k.ljust(_KLEN) + "  " + lclstr[k].ljust(_VLEN) + "\n")

        pdiff = {}
        for key in l_s:
            try:
                if lclstr[key] != rclstr[key]:
                    pdiff[key] = (lclstr[key], rclstr[key])
            except KeyError:
                # A property is missing in Right cluster, ignore
                None

        if pdiff:
            if args.missingprops == 'y' and (lmiss or rmiss):
                rlst.append("\n"*2 + "*"*_HLEN + "Property differences" + "*"*_HLEN)
            rlst.append("\nProperty".ljust(_KLEN) + "   " +
                        "Left Cluster".ljust(_VLEN) + "   " +
                        "Right Cluster".ljust(_VLEN))
            rlst.append("\n" + _PSEP*_KLEN + "  " + _PSEP*_VLEN  + "  " + _PSEP*_VLEN  + "\n")
            for k, v in pdiff.items():
                rlst.append(k.ljust(_KLEN) + "  " + v[0].ljust(_VLEN) + "  " + v[1].ljust(_VLEN) + "\n")
        if rlst:
            resfile = os.path.join(resdir, '.'.join((rfl.split('.')[0], 'txt')))
            with open(resfile, 'w') as wfl:
                wfl.writelines(rlst)
            print("%s differences have been saved to file %s" %(rfl, resfile))

def runmain():
    fldir = os.environ['HOME'] or '/tmp'
    parser = argparse.ArgumentParser(description="Compare property differences between 2 clusters")
    parser.add_argument('--left', default='/tmp/confL',
                        help='Left cluster folder with xml property files, Default: /tmp/confL')
    parser.add_argument('--right', default='/tmp/confR',
                        help='Left cluster folder with xml property files, Default: /tmp/confR')
    parser.add_argument('--resultdir', default=fldir,
                        help="Folder to save the comparison files, Default: %s" %fldir)
    parser.add_argument('--missingprops', default='y', choices={'y', 'n'},
                        help="Print missing properties between clusters, Default: y")
    args = parser.parse_args()

    # Left cluster is considered the Source/Template against which right cluster is compared
    print("Start comparing cluster properties: %s" %datetime.now().strftime("%m%d%Y_%H:%M:%S"))

    if not os.path.isdir(args.left):
        print('Left cluster path is not a folder')
        sys.exit(1)

    if not os.path.isdir(args.right):
        print('Right cluster path is not a folder')
        sys.exit(1)

    if args.left == args.right:
        print('Left and Right clusters cannot point to the same folder')
        sys.exit(1)

    # compare only xml files
    lfiles = {os.path.basename(f) for f in glob.glob(args.left + "**/*.xml")}
    rfiles = {os.path.basename(f) for f in glob.glob(args.right + "**/*.xml")}
    lmiss = rfiles - lfiles
    rmiss = lfiles - rfiles
    if lmiss:
        print("\n"*2 + "*"*_HLEN + "Following files are missing from Left Cluster" + "*"*_HLEN)
        for xfl in lmiss:
            print(xfl)

    if rmiss:
        print("\n"*2 + "*"*_HLEN + "Following files are missing from Right Cluster" + "*"*_HLEN)
        for xfl in rmiss:
            print(xfl)

    # Results stored in folder with name "right"_"left"
    resdir = '/'.join((args.resultdir,
                       os.path.basename(args.left) + '_' + os.path.basename(args.right)))
    #print(resdir)
    try:
        if not os.path.isdir(resdir):
            os.mkdir(resdir)

        # Compare files that exist on both folders
        flist = lfiles.intersection(rfiles)
        #print(flist)
        compare_clust(args, resdir, flist)
    except Exception as err:
        print(err)
        print("Unable to compare cluster properties. Exiting....")
        sys.exit(1)
    print("\nEnd comparing cluster properties: %s" %datetime.now().strftime("%m%d%Y_%H:%M:%S"))
    sys.exit(0)


if __name__ == '__main__':
    runmain()
