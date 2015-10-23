# -*- coding: utf-8 -*-
#
# This file is part of INSPIRE.
# Copyright (C) 2015 CERN.
#
# INSPIRE is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# INSPIRE is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with INSPIRE; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""BibFormat element - Enhanced INSPIRE MARCXML"""

from invenio.bibrecord import record_xml_output, record_get_field_instances, field_get_subfield_instances
from invenio.search_engine import perform_request_search
from invenio.bibrank_citation_indexer import get_tags_config as _get_tags_config, get_recids_matching_query
from invenio.refextract_linker import find_doi, find_journal, find_reportnumber, find_book, find_isbn
from invenio.dbquery import run_sql

def get_institution_ids(text, INSTITUTION_CACHE={}):
    # HACK: I know... I am sorry for that. It's for a good cause
    # FIXME: use redis
    if text not in INSTITUTION_CACHE:
        INSTITUTION_CACHE[text] = perform_request_search(cc='Institutions', p='institution:"%s"' % text)
    return INSTITUTION_CACHE[text]

def reference2citation_element(subfields):
    citation_element = {}
    for code, value in subfields:
        if code == 'a':
            citation_element['doi_string'] = value
        elif code == 't':
            citation_element['title'] = value
        elif code == 'i':
            citation_element['isbn'] = value
        elif code == 'r':
            citation_element['report_num'] = value
        elif code == 'y':
            citation_element['year'] = value
        elif code == 's':
            try:
                journal, volume, page = value.split(',')
                citation_element['journal_title'] = journal
                citation_element['volume'] = volume
                citation_element['page'] = page
            except ValueError:
                pass
    if 'journal_title' in citation_element:
        # HACK: to work around clash between find_journal() and find_book()
        citation_element['title'] = citation_element['journal_title']
    return citation_element


def get_matched_id(subfields):
    citation_element = reference2citation_element(subfields)
    if 'doi_string' in citation_element:
        recids = find_doi(citation_element)
        if len(recids) == 1:
            return recids.pop()
    if 'journal_title' in citation_element and 'year' in citation_element:
        recids = find_journal(citation_element)
        if len(recids) == 1:
            return recids.pop()
    if 'report_num' in citation_element:
        recids = find_reportnumber(citation_element)
        if len(recids) == 1:
            return recids.pop()
    if 'isbn' in citation_element:
        recids = find_isbn(citation_element)
        if len(recids) == 1:
            return recids.pop()
    #if 'title' in citation_element:
        #recids = find_book(citation_element)
        #if len(recids) == 1:
            #return recids.pop()
    return None


def format_element(bfo, oai=0):
    """Produce MARCXML with enhanced fields.

    Adds 100/700 $x with Record ID of linked HepName,
                 $y with True/False if the signature is claimed
                 $z with Record ID of institution
         371/110 $z with Record ID of institution
         502     $z with Record ID of institution
         999C5   $0 with on the fly discovered Record IDs (not for books)
         773     $0 with Record ID of corresponding Book or Proceeding
                 $1 with Record ID of corresponding Journal
                 $2 with Record ID of corresponding Conference
         693/710 $0 with Record ID of corresponding experiment
    """
    record = bfo.get_record()
    recid = bfo.recID

    # Let's add signatures
    for field in record_get_field_instances(record, '100') + record_get_field_instances(record, '700'):
        subfields = field_get_subfield_instances(field)
        subfield_dict = dict(subfields)
        if 'a' in subfield_dict:
            author_name = subfield_dict['a']
            rows = run_sql("SELECT personid, flag FROM aidPERSONIDPAPERS WHERE bibrec=%s AND name=%s AND flag>-2", (recid, author_name))
            if rows:
                personid, flag = rows[0]
                canonical_name = run_sql("SELECT data FROM aidPERSONIDDATA WHERE personid=%s AND tag='canonical_name'", (personid, ))
                if canonical_name:
                    id = perform_request_search(p='035__a:"%s"' % canonical_name[0], cc='HepNames')
                    if id:
                        subfields.append(('x', '%i' % id[0]))
                        subfields.append(('y', '%i' % (flag > 0)))

        # And matched affiliations
        if 'u' in subfield_dict:
            for code, value in subfields:
                if code == 'u':
                    ids = get_institution_ids(value)
                    if len(ids) == 1:
                        subfields.append(('z', '%i' % ids[0]))

    # Thesis institution
    for field in record_get_field_instances(record, '502'):
        if 'u' in subfield_dict:
            for code, value in subfields:
                if code == 'c':
                    ids = get_institution_ids(value)
                    if len(ids) == 1:
                        subfields.append(('z', '%i' % ids[0]))

    # Enhance affiliation in HepNames and Jobs
    for field in record_get_field_instances(record, '371') + record_get_field_instances(record, '110'):
        subfields = field_get_subfield_instances(field)
        subfield_dict = dict(subfields)
        if 'a' in subfield_dict:
            for code, value in subfields:
                if code == 'a':
                    ids = get_institution_ids(value)
                    if len(ids) == 1:
                        subfields.append(('z', '%i' % ids[0]))

    # Enhance citation
    for field in record_get_field_instances(record, '999', ind1='C', ind2='5'):
        subfields = field_get_subfield_instances(field)
        subfield_dict = dict(subfields)
        if '0' not in subfield_dict:
            matched_id = get_matched_id(subfields)
            if matched_id:
                subfields.append(('0', str(matched_id)))

    # Enhance CNUMs and Journals
    for field in record_get_field_instances(record, '773'):
        subfields = field_get_subfield_instances(field)
        for code, value in subfields:
            if code == 'w':
                # Conference CNUMs
                recids = perform_request_search(p='111__g:"%s"' % value, cc='Conferences')
                if len(recids) == 1:
                    subfields.append(('2', str(recids.pop())))
                recids = perform_request_search(p='773__w:"%s" 980:PROCEEDINGS' % value)
                if recid in recids:
                    # We remove this very record, since it can be a proceedings
                    recids.remove(recid)
                if len(recids) == 1:
                    if recids
                    subfields.append(('0', str(recids.pop())))
            elif code == 'p':
                # Journal title
                recids = perform_request_search(p='711__a:"%s"' % value, cc='Journals')
                if len(recids) == 1:
                    subfields.append(('1', str(recids.pop())))
            elif code == 'z':
                # ISBN
                recids = find_isbn({'isbn': value})
                if len(recids) == 1:
                    subfields.append(('0', str(recids.pop())))

    # Enhance Experiments
    for field in record_get_field_instances(record, '693'):
        subfields = field_get_subfield_instances(field)
        for code, value in subfields:
            if code == 'e':
                recids = perform_request_search(p='119__a:"%s"' % value, cc='Experiments')
                if len(recids) == 1:
                    subfields.append(('0', str(recids.pop())))

    # Enhance Experiments
    for field in record_get_field_instances(record, '710'):
        subfields = field_get_subfield_instances(field)
        for code, value in subfields:
            if code == 'g':
                recids = perform_request_search(p='119__a:"%s"' % value, cc='Experiments')
                if len(recids) == 1:
                    subfields.append(('0', str(recids.pop())))

    formatted_record = record_xml_output(record)
    if oai:
        formatted_record = formatted_record.replace("<record>", "<marc:record xmlns:marc=\"http://www.loc.gov/MARC21/slim\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd\" type=\"Bibliographic\">\n     <marc:leader>00000coc  2200000uu 4500</marc:leader>")
        formatted_record = formatted_record.replace("<record xmlns=\"http://www.loc.gov/MARC21/slim\">", "<marc:record xmlns:marc=\"http://www.loc.gov/MARC21/slim\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd\" type=\"Bibliographic\">\n     <marc:leader>00000coc  2200000uu 4500</marc:leader>")
        formatted_record = formatted_record.replace("</record", "</marc:record")
        formatted_record = formatted_record.replace("<controlfield", "<marc:controlfield")
        formatted_record = formatted_record.replace("</controlfield", "</marc:controlfield")
        formatted_record = formatted_record.replace("<datafield", "<marc:datafield")
        formatted_record = formatted_record.replace("</datafield", "</marc:datafield")
        formatted_record = formatted_record.replace("<subfield", "<marc:subfield")
        formatted_record = formatted_record.replace("</subfield", "</marc:subfield")
    return formatted_record

def escape_values(bfo):
    """
    Called by BibFormat in order to check if output of this element
    should be escaped.
    """
    return 0