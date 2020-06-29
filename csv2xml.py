# Copyright 2019 BlueCat Networks (USA) Inc. and its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
# encoding: utf-8
import os
import sys
import logging
import traceback
import logging.handlers
import csv
import xml.etree.ElementTree as ET
from xml.dom import minidom

VERSION = "1.3"
DOCTYPE_QUALIFIED_NAME = 'data'
DOCTYPE_PUBLIC_ID = "-//BlueCat Networks/Proteus Migration Specification 9.0//EN"
DOCTYPE_SYSTEM_ID = "http://www.bluecatnetworks.com/proteus-migration-9.0.dtd"
LOG_PATH = 'logs'
LOG_NAME = 'convert'


class Header():
    OPTYPE = 'OpType'
    CONFIG = 'Config'
    VIEW = 'View'
    PARENT_ZONE = 'ParentZone'
    ZONE_DEPLOY_FLAG = 'ZoneDeployFlag'
    NAME = 'Name'
    RECORD_TYPE = 'RecordType'
    ON_EXIST = 'on-exist'
    TTL = 'TTL'
    HOST_ADDRESS = 'HostAddress'
    R_DATA = 'Rdata'
    NAPTR_ORDER = 'naptr-order'
    NAPTR_PRE = 'naptr-Preference'
    NAPTR_SERVICE = 'naptr-Service'
    NAPTR_REGEXP = 'naptr-RegExp'
    NAPTR_REP = 'naptr-Replacement'
    NAPTR_FLAGS = 'naptr-Flags'
    SRV_PRIO = 'SRV-Priority'
    SRV_WEI = 'SRV-Weight'
    SRV_PORT = 'SRV-Port'
    SRV_HOST = 'SRV-Host'

    @classmethod
    def all(cls):
        return [value for name, value in vars(cls).items() if name.isupper()]

class OpType():
    REMARK = "remark"
    CONFIG = "config"
    VIEW = "view"
    RECORD = "record"
    ZONE = 'zone'


class RecordType():
    HOST = 'host'
    SRV = 'srv'
    NAPTR = 'naptr'
    GENERIC = 'generic-record'


class GenericRecordType():
    NS = 'NS'
    AAA = 'AAA'
    A = 'A'

###
# Exception
###
class FieldEmptyException(Exception):
    def __init__(self, title):
        self.msg = "{} is empty.".format(title)

    def __str__(self):
        return(self.msg)

class HeaderMissingException(Exception):
    def __init__(self, header):
        self.msg = "Header '{}' is missing.".format(header)

    def __str__(self):
        return(self.msg)

class CsvToXml():
    def __init__(self):
        self.tree = ET.ElementTree()
        self.root = ET.Element('data')
        self.tree._setroot(self.root)

    def __handle_configuation(self, name, on_exist=""):
        if not name:
            raise FieldEmptyException(Header.CONFIG)
        configurations = self.root.findall(
            "./{}[@name='{}']".format("configuration", name))
        if len(configurations) > 0:
            return configurations[0]
        # Create configuration
        configuration = ET.SubElement(self.root, "configuration")
        configuration.set('name', name)
        if on_exist:
            configuration.set('on-exist', on_exist)
        return configuration

    def __handle_view(self, config_name, name, on_exist=""):
        if not name:
            raise FieldEmptyException(Header.VIEW)
        configuration = self.__handle_configuation(config_name)
        views = configuration.findall(
            "./{}[@name='{}']".format(OpType.VIEW, name))
        if len(views) > 0:
            return views[0]
        # Create view
        view = ET.SubElement(configuration, OpType.VIEW)
        view.set('name', name)
        if on_exist:
            view.set('on-exist', on_exist)
        return view

    def __handle_zone(self, config_name, view_name, full_zone_name, is_deploy=False, on_exist=""):
        view = self.__handle_view(config_name, view_name)
        list_zone = full_zone_name.split('.')
        list_zone.reverse()
        parent = view
        # Example after reverse:
        # list_zone = [corp1, test1, sub1]
        for index in range(len(list_zone)):
            p_zone = list_zone[index]
            zones = parent.findall(
                "./{}[@name='{}']".format(OpType.ZONE, p_zone))
            if len(zones) > 0:
                parent = zones[0]
            else:
                zone = ET.SubElement(parent, OpType.ZONE)
                zone.set('name', p_zone)
                if index == len(list_zone)-1 and is_deploy:
                    zone.set('deployable', str(is_deploy).lower())
                if index == len(list_zone)-1 and on_exist:
                    zone.set('on-exist', on_exist)
                parent = zone
        return parent

    def __handle_record(self, row):
        config_name = row.get(Header.CONFIG)
        view_name = row.get(Header.VIEW)
        name = row.get(Header.NAME)
        parent_zone = row.get(Header.PARENT_ZONE)
        if not parent_zone:
            raise FieldEmptyException(Header.PARENT_ZONE)
        record_type = row.get(Header.RECORD_TYPE)
        if not record_type:
            raise FieldEmptyException(Header.RECORD_TYPE)
        zone = self.__handle_zone(config_name, view_name, parent_zone)
        # Lower record_type
        record_type = record_type.lower()
        if record_type == RecordType.HOST:
            self.__handle_record_host(
                zone, name,
                row.get(Header.HOST_ADDRESS),
                row.get(Header.TTL),
                row.get(Header.ON_EXIST))
        elif record_type == RecordType.SRV:
            self.__handle_record_srv(
                zone, name,
                row.get(Header.SRV_PRIO),
                row.get(Header.SRV_WEI),
                row.get(Header.SRV_PORT),
                row.get(Header.SRV_HOST),
                row.get(Header.TTL),
                row.get(Header.ON_EXIST)
            )
        elif record_type == RecordType.NAPTR:
            self.__handle_record_naptr(
                zone, name,
                row.get(Header.NAPTR_ORDER),
                row.get(Header.NAPTR_PRE),
                row.get(Header.NAPTR_SERVICE),
                row.get(Header.NAPTR_REP),
                row.get(Header.NAPTR_FLAGS),
                row.get(Header.TTL),
                row.get(Header.ON_EXIST),
                row.get(Header.NAPTR_REGEXP)
            )
        elif record_type in ['ns', 'a', 'aaaa']:
            self.__handle_record_generic(
                zone, name, record_type,
                row.get(Header.R_DATA),
                row.get(Header.TTL),
                row.get(Header.ON_EXIST)
            )

    def __handle_record_host(self, zone, name, address, ttl, on_exist):
        # host_records = zone.findall(
        #     "./{}[@name='{}']".format(RecordType.HOST, name))
        # if len(host_records) > 0:
        #     return host_records[0]
        if not address:
            raise FieldEmptyException(Header.HOST_ADDRESS)
        host_record = ET.SubElement(zone, RecordType.HOST)
        host_record.set('name', name)
        host_record.set('address', address)
        if ttl:
            host_record.set('ttl', ttl)
        if on_exist:
            host_record.set('on-exist', on_exist)
        return host_record

    def __handle_record_srv(self, zone, name, priority, weight, port, host, ttl, on_exist):
        # srv_records = zone.findall(
        #     "./{}[@name='{}']".format(RecordType.SRV, name))
        # if len(srv_records) > 0:
        #     return srv_records[0]
        srv_record = ET.SubElement(zone, RecordType.SRV)
        srv_record.set('name', name)
        srv_record.set('priority', priority)
        srv_record.set('weight', weight)
        srv_record.set('port', port)
        srv_record.set('host', host)
        if ttl:
            srv_record.set('ttl', ttl)
        if on_exist:
            srv_record.set('on-exist', on_exist)
        return srv_record

    def __handle_record_naptr(self, zone, name, order, preference, service, replacement, flags, ttl, on_exist, regexp):
        # naptr_records = zone.findall(
        #     "./{}[@name='{}']".format(RecordType.NAPTR, name))
        # if len(naptr_records) > 0:
        #     return naptr_records[0]
        naptr_record = ET.SubElement(zone, RecordType.NAPTR)
        naptr_record.set('name', name)
        if order:
            naptr_record.set('order', order)
        if preference:
            naptr_record.set('preference', preference)
        if service:
            naptr_record.set('service', service)
        if replacement:
            naptr_record.set('replacement', replacement)
        if flags:
            naptr_record.set('flags', flags)
        if regexp:
            naptr_record.set('regexp', regexp)
        if ttl:
            naptr_record.set('ttl', ttl)
        if on_exist:
            naptr_record.set('on-exist', on_exist)
        return naptr_record

    def __handle_record_generic(self, zone, name, type, rdata, ttl, on_exist):
        # generic_records = zone.findall(
        #     "./{}[@name='{}']".format(RecordType.GENERIC, name))
        # if len(generic_records) > 0:
        #     return generic_records[0]
        if not rdata:
            raise FieldEmptyException(Header.R_DATA)
        generic_record = ET.SubElement(zone, RecordType.GENERIC)
        generic_record.set('name', name)
        generic_record.set('type', type.upper())
        generic_record.set('rdata', rdata)
        if ttl:
            generic_record.set('ttl', ttl)
        if on_exist:
            generic_record.set('on-exist', on_exist)
        return generic_record

    def extract(self, row, line_num):
        optype = row.get(Header.OPTYPE)
        if not optype and [value for value in list(row.values()) if value]:
            # If optype is empty and has another value in this row
            # Raise exception
            raise FieldEmptyException(Header.OPTYPE)
        elif optype == OpType.REMARK:
            pass
        elif optype == OpType.CONFIG:
            self.__handle_configuation(
                row.get(Header.CONFIG), row.get(Header.ON_EXIST))
        elif optype == OpType.VIEW:
            self.__handle_view(row.get(Header.CONFIG), row.get(
                Header.VIEW), row.get(Header.ON_EXIST))
        elif optype == OpType.ZONE:
            if not row.get(Header.NAME):
                raise FieldEmptyException(Header.NAME)
            self.__handle_zone(
                row.get(Header.CONFIG), row.get(Header.VIEW), row.get(
                    Header.NAME) + '.' + row.get(Header.PARENT_ZONE),
                bool(row.get(Header.ZONE_DEPLOY_FLAG)), row.get(Header.ON_EXIST))
        elif optype == OpType.RECORD:
            self.__handle_record(row)

    def get_out_xml(self, encode_type='utf8'):
        return ET.tostring(self.root, encoding=encode_type).decode(encode_type)

    def write_to_file(self, file_name, encode_type='utf-8'):
        with open(file_name, 'wb') as f:
            xml_data = minidom.parseString(ET.tostring(self.root))
            dt = minidom.getDOMImplementation('').createDocumentType(
                DOCTYPE_QUALIFIED_NAME, DOCTYPE_PUBLIC_ID, DOCTYPE_SYSTEM_ID)
            xml_data.insertBefore(dt, xml_data.documentElement)
            f.write(xml_data.toprettyxml(indent="  ", encoding=encode_type))


def execute(file_name, logger, out_file):
    csv_to_xml = CsvToXml()
    with open(file_name, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        validate_header(csv_reader.fieldnames)
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            logger.debug("[line: {}] - {}".format(line_count + 1, row))
            try:
                csv_to_xml.extract(row, line_count)
            except FieldEmptyException as emp_except:
                logger.error(
                    "[line: {}] - {}".format(line_count + 1, str(emp_except)))
            line_count += 1
        logger.info('Processed {} lines.'.format(line_count))
    csv_to_xml.write_to_file(out_file)


def get_logger(is_debug=False):
    if not os.path.exists(LOG_PATH):
        os.mkdir(LOG_PATH)

    if is_debug:
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger()
    else:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("csv2xml")

    import datetime
    handler = logging.FileHandler(
        LOG_PATH + "/" + LOG_NAME + '_' + datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S") + '.log')
    if os.path.isfile(LOG_PATH + "/" + LOG_NAME):
        handler.doRollover()
    log_formater = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
    handler.setFormatter(log_formater)
    logger.addHandler(handler)
    return logger


def validate_header(actual_headers):
    """Validate Headers Get From CSV File
    
    Keyword arguments:
    argument -actual_headers- list header get from CSV File
    Return: Exception if header expect not in list header from CSV file
    - Do nothing if actual_headers is enough
    """
    expect_headers = Header.all()
    for header in expect_headers:
        if header not in actual_headers:
            raise HeaderMissingException(header)

def validate_cmd_input(parser):
    # Get comment input and options
    (options, args) = parser.parse_args()
    out_file = options.output_filename
    is_debug = True if options.debug else False

    csv_suffix = '.csv'
    if len(args) < 1:
        parser.print_help()
        parser.exit()
    file_name = args[0]
    if not os.path.isfile(file_name) or not file_name.endswith(csv_suffix):
        parser.error(msg="{}: No such file csv".format(file_name))
    return file_name, out_file, is_debug


if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser(version="%prog " + VERSION,
                          usage='%prog [options] SOURCE_CSV')
    parser.disable_interspersed_args()
    parser.add_option('-o', '--output', dest='output_filename', type='str', default='output.xml',
                      help="Output XML file name (Default: output.xml)")
    parser.add_option("-d", action="store_true", dest="debug",
                      help="Enable debug mode (Default: Disable)")

    in_file, out_file, is_debug = validate_cmd_input(parser)
    logger = get_logger(is_debug)

    try:
        logger.info("Start to convert file '{}'".format(in_file))
        logger.debug("Python version: {}".format(sys.version))
        execute(in_file, logger, out_file)
    except HeaderMissingException as header_ex:
        logger.error(str(header_ex))
    except Exception:
        logger.error(traceback.format_exc())
    finally:
        logger.info("Finished")
