#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import csv
import random
import multiprocessing

from argparse import ArgumentParser
from bs4 import BeautifulSoup
from uuid import uuid4
from zipfile import ZipFile
from StringIO import StringIO

ZIP_FILES_COUNT = 50
XML_FILES_COUNT = 100
GENERATED_FILES_DIR = 'results_dir'


def create_one_zip_file(zip_archive_number):
    inMemoryOutputFile = StringIO()
    zipFile = ZipFile(inMemoryOutputFile, 'w') 

    for xml_file_number in xrange(1, XML_FILES_COUNT + 1):
        soup = BeautifulSoup(features='xml')
        soup.append(soup.new_tag("root"))
        var_id = soup.new_tag('var', value=str(uuid4()))
        var_id['name'] = 'id'
        soup.root.append(var_id)
        var_level = soup.new_tag('var', value=random.randint(1, 100))
        var_level['name'] = 'level'
        soup.root.append(var_level)
        soup.root.append(soup.new_tag('objects'))
        for i in xrange(1, random.randint(1, 10)):
            new_object = soup.new_tag('object')
            new_object['name'] = str(uuid4())
            soup.root.objects.append(new_object)

        zipFile.writestr('%s.xml' % xml_file_number, str(soup))

    zipFile.close()
    inMemoryOutputFile.seek(0)

    with open('%s/%s.zip' % (GENERATED_FILES_DIR, zip_archive_number), 'w') as fd:
        fd.write(inMemoryOutputFile.getvalue())


def parse_one_zip_file(zip_file):
    levels_result = []
    object_names_result = []
    with ZipFile(zip_file, 'r') as zip_file_fd:
        for xml_file in zip_file_fd.namelist():
            soup = BeautifulSoup(zip_file_fd.read(xml_file), features='xml')
            xml_vars = {tag['name']: tag['value'] for tag in soup.root.find_all('var')}
            levels_result.append((xml_vars['id'], xml_vars['level']))

            for xml_object in soup.root.objects.find_all('object'):
                object_names_result.append((xml_vars['id'], xml_object['name']))

    return (levels_result, object_names_result)


def create(args):
    if not os.path.exists(GENERATED_FILES_DIR):
        os.makedirs(GENERATED_FILES_DIR)
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.map(create_one_zip_file, xrange(1, ZIP_FILES_COUNT + 1))
    pool.close()
    pool.join()


def parse(args):
    levels_writer = csv.writer(open('%s/levels.csv' % GENERATED_FILES_DIR, 'w'))
    object_names_writer = csv.writer(open('%s/object_names.csv' % GENERATED_FILES_DIR, 'w'))

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    result = pool.imap(parse_one_zip_file, glob.iglob("%s/*.zip" % GENERATED_FILES_DIR))
    for levels_result, object_names_result in result:
        levels_writer.writerows(levels_result)
        object_names_writer.writerows(object_names_result)

    pool.close()
    pool.join()


if __name__ == '__main__':
    parser = ArgumentParser(description='Xml generator and parser')
    sp = parser.add_subparsers()
    sp_create = sp.add_parser('create', help='Create zip archives')
    sp_create.set_defaults(func=create)

    sp_parse = sp.add_parser('parse', help='Parse zip archives')
    sp_parse.set_defaults(func=parse)

    args = parser.parse_args()

    args.func(args)
