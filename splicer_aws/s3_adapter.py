# Splicer adapter for S3
# Author: Scott Robertson
# (c) triv.io all rights reserved

import os
from os.path import join
import re
from itertools import chain


from tempfile import SpooledTemporaryFile

import boto
from boto.s3.key import Key

from splicer import Schema, Field
from splicer.path import pattern_regex
from splicer import codecs

class S3Adapter(object):
  def __init__(self, **relations):
    self._relations = {
      name:S3Table(name, **args) for name,args in relations.items()
    }

  @property
  def relations(self):
    return [
      (name, relation.schema)
      for name, relation in self._relations.items()
    ]

  def get_relation(self, name):
    return self._relations.get(name)

class S3File(object):
  def __init__(self, bucket, path):
    self.key = Key(bucket, path)
    self._fp = None

  @property
  def file(self):
    if self._fp is None:
      # only go to disk if we get more than 1MB of data
      self._fp = SpooledTemporaryFile(max_size=1024**2)
      # todo: only read the key if we actually try to iterate
      # this file
      self.key.get_contents_to_file(self._fp)
      self._fp.seek(0)
    return self._fp

  def __iter__(self):
    return iter(self.file)

  def read(self, bytes):
    return self.file.read(bytes)

class S3Table(object):
  def __init__(self,  name, bucket,  **options):

    # todo: allow setting by url and use AWS_KEYS and Secrets
    self.bucket = boto.connect_s3().get_bucket(bucket)
    self.name = name
    self.delimiter=options.pop('delimiter','/')

    prefix = options.pop('prefix', '/')
    if not prefix.endswith(self.delimiter):
      prefix += self.delimiter
    self.prefix = prefix

    self.marker = options.pop('marker', None)

    pattern = options.pop('pattern', None)
    if pattern:
      while pattern.startswith(self.delimiter):
        pattern = pattern[len(self.delimiter):]

      self.pattern_regex, self.pattern_columns = pattern_regex(
        self.prefix + pattern
      )

    else:
      self.pattern_regex = None
      self.pattern_columns = []

    self.content_column = options.pop('content_column', None)
    self.filename_column = options.pop('filename_column', None)
  
    self.decode = options.pop('decode', "none")

    self._schema = None
    if 'schema' in options:
      self._schema = Schema(**options.pop('schema'))

    if options:
      raise ValueError("Unrecognized options {}".format(options.keys()))


  @property
  def schema(self):
    if self._schema is None:
      fields = [
        Field(name=name, type="STRING") for name in self.pattern_columns
      ]

      if self.content_column:
        fields.append(Field(name=self.content_column, type='BINARY'))

      if self.filename_column:
        fields.append(Field(name=self.filename_column, type='STRING'))

      if self.decode != "none":
        fields.extend(
          self.fields_from_content(self.decode)
        )
      self._schema = Schema(name=self.name, fields=fields)


    return self._schema


  def keys(self):
    return self.bucket.list(self.prefix, marker=self.marker)
    #return (
    #  join(root,f)
    #  for root, dirs, files in os.walk(self.root_dir)
    #  for f in files
    #)

  def match_info(self):
    """
    Returns: tuple((<extracted1>,...,<extractedX>), path)
    for each file that matches the FileTable's pattern_regex.
    If no regex is specified returns tuple((), path) for all
    files under the root_dir.

    """

    if not self.pattern_regex:
      for key in self.keys():
        yield (), key.name
    else:
      for key in self.keys():
        m = self.pattern_regex.match(key.name)
        if m:
          yield m.groups(), key.name


  def fields_from_content(self, decode):
    """
    Returns the schema for the first matching file
    under the root_dir.
    """

    try:
      partition_data, path  = self.match_info().next()
    except StopIteration:
      return []

    stream = S3File(self.bucket, path)

    relation = codecs.relation_from(stream, decode)
    if relation:
      return relation.schema.fields
    else:
      # todo: consider defaulting to 'application/octet-stream'
      return []

  def extract_function(self):
    """
    Returns a function that will extract the data from a
    file path.
    """

    def identity(partition_info, path):
      yield partition_info

    funcs = [identity]

 
    if self.filename_column:
      def filename(partition_info, path):
        yield  partition_info + (path,)

      funcs.append(filename)

    if self.decode != "none":
      def decode(partition_info, path):
        stream = S3File(self.bucket, path)

        relation = codecs.relation_from(stream, self.decode)
        return (
          partition_info + tuple(row)
          for row in relation
        )
      funcs.append(decode)

    if len(funcs) == 1:
      return funcs[0]

    def extract(partition_info, path):
      """
      Return one or more rows by composing the functions.
      """

      rows = funcs[0](partition_info,path)

      for f in funcs[1:]:
        rows = chain(*(
          iter(f(row, path))
          for row in rows
        ))

      return rows

    return extract



  def __iter__(self):
    extract = self.extract_function()

    for partition_info, path in self.match_info():
      for row in extract(partition_info, path):
        yield row


