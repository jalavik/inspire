#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2014 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""
    name:           bibfilter_oai_inspire2cds
    decription:     Program to filter and analyse MARCXML records
                    harvested from external OAI sources, in order to determine
                    which action needs to be taken (insert, holding-area, etc)

                    Based on bibfilter_oaiarXiv2inspire
"""
import os
import sys
import getopt

from invenio.bibrecord import (record_get_field_values,
                               record_xml_output)
from invenio.bibtask import write_message

from harvestingkit.invenio_package.converters import Inspire2CDSRecord
from harvestingkit.invenio_package.parsers import InvenioOAIParser


def main(args):
    usage = """
    name:           bibfilter_oai_inspire2cds.py
    description:    Program to filter and analyze MARCXML records
                    harvested from external OAI sources
    usage:
                    bibfilter_oai_inspire2cds [-nh] MARCXML-FILE
    options:
                -n  forces the script not to check if the record exists in
                    the database (useful when re-harvesting existing record)
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "nh", [])
    except getopt.GetoptError, err_obj:
        sys.stderr.write("Error:" + err_obj + "\n")
        print usage
        sys.exit(1)

    skip_recid_check = False

    for opt, opt_value in opts:
        if opt in ['-n']:
            skip_recid_check = True
        if opt in ['-h']:
            print usage
            sys.exit(0)

    if len(args) != 1:
        sys.stderr.write("Error: Missing MARCXML to analyse")
        sys.exit(1)

    input_filename = args[0]

    if not os.path.exists(input_filename):
        sys.stderr.write("Input file %s not a valid filename for input."
                         % (input_filename,))
        sys.exit(1)

    parsed_file = InvenioOAIParser(path=input_filename,
                                   record_filter=Inspire2CDSRecord)
    parsed_file.parse()
    parsed_records = parsed_file.get_records()

    insert_records = []
    append_records = []
    error_records = []

    for record in parsed_records:
        # Step 1: Attempt to match the record to those already in CDS
        try:
            recid = record.record["001"][0][3]
            query = "035:%(system)s 035:%(recid)s"
            res = record.match(query,
                               system="Inspire")
        except (KeyError, IndexError):
            record.logger.error('Error: Cannot process record without 001:recid')
            error_records.append(record)
            continue

        if skip_recid_check or not res:
            record.logger.info("Record %s does not exist: inserting"
                               % (recid,))
            insert_records.append(record.get_record())
        else:
            record.logger.info("Record %s found: %r"
                               % (recid, res))

    for record in parsed_file.deleted_records:
        recid = record_get_field_values(record.record,
                                        tag="035",
                                        code="a")[0].split(":")[-1]
        query = "035:%(system)s 035:%(recid)s"
        res = record.match(query,
                           system="Inspire",
                           recid=recid)
        if res:
            # Record exists and we should then delete it
            record.logger.info("Record %s exists. Delete it" % (recid,))
            append_records.append(record.get_record())

    # Output results. Create new files, if necessary.
    if input_filename[-4:].lower() == '.xml':
        input_filename = input_filename[:-4]

    try:
        write_record_to_file("%s.insert.xml" % (input_filename,), insert_records)
        print("%s.insert.xml" % (input_filename,))
        write_message("Number of records to insert:  %d\n"
                      % (len(insert_records),))
        write_record_to_file("%s.append.xml" % (input_filename,), append_records)
        print("%s.append.xml" % (input_filename,))
        write_message("Number of records to append:  %d\n"
                      % (len(append_records),))
        write_record_to_file("%s.errors.xml" % (input_filename,), error_records)
        print("%s.errors.xml" % (input_filename,))
        write_message("Number of records with errors:  %d\n"
                      % (len(append_records),))
    except Exception, e:
        from invenio.errorlib import register_exception
        register_exception()
        raise e


def write_record_to_file(filename, record_list):
    """
    Writes a new MARCXML file to specified path from a list of records.
    """
    if len(record_list) > 0:
        out = []
        out.append("<collection>")
        for record in record_list:
            if record != {}:
                out.append(record_xml_output(record))
        out.append("</collection>")
        if len(out) > 2:
            file_fd = open(filename, 'w')
            file_fd.write("\n".join(out))
            file_fd.close()


if __name__ == '__main__':
    main(sys.argv[1:])
