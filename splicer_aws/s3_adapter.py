# Splicer adapter for S3
# Author: Scott Robertson
# (c) triv.io all rights reserved

import os
from os.path import join
import re

from itertools import chain
from functools import partial
from tempfile import SpooledTemporaryFile

import boto
from boto.s3.key import Key

from splicer import Relation, Schema
from splicer.ast import And, Const, BinaryOp, Function, Or, Tuple,Var, SelectionOp
from splicer.path import pattern_regex
from splicer.adapters import Adapter

class S3Adapter(Adapter):
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

  def has(self, name):
    return name in self._relations

  def evaluate(self, loc):
    name = loc.node().name
    relation = self._relations[name]

    loc = loc.replace(
      lambda ctx: s3_keys(
        relation.bucket_name, 
        relation.prefix,
        relation.delimiter,
        relation.validate_url_for
      )
    )

    if relation.pattern:
      loc = loc.replace(
        Function(
          'extract_path', 
          loc.node(), 
          Const(relation.root() + relation.pattern),
          Const('url')
        )
      )

    #todo rewrite select queries, also update prefix with where parameters if
    # possible
    return loc.leftmost_descendant()



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
    self.anon = options.pop('anon', False)
    if self.anon:
      self.validate_url_for = None
    else:
      self.validate_url_for = 60*60*24

    self.connection = boto.connect_s3(
      anon = self.anon,
      is_secure = options.pop('is_secure', True)
    ) 

    self.bucket_name = bucket

    self.name = name



    self.delimiter=options.pop('delimiter','/')
    

    prefix = options.pop('prefix', '/')
    if not prefix.endswith(self.delimiter):
      prefix += self.delimiter
    self.prefix = prefix

    self.marker = options.pop('marker', None)

    self.pattern = options.pop('pattern', None)

    self.content_column = options.pop('content_column', None)
    self.filename_column = options.pop('filename_column', None)
  
    self.decode = options.pop('decode', "none")

    self._schema = None
    if 'schema' in options:
      self._schema = Schema(**options.pop('schema'))

    if options:
      raise ValueError("Unrecognized options {}".format(options.keys()))

  def root(self):
    return http_url(self.connection,self.bucket_name, self.prefix)




def s3_keys(bucket_name, prefix, delimiter, validate_url_for):
  conn   = boto.connect_s3(anon=True)
  bucket = conn.get_bucket(bucket_name)
  
  generate_url = generate_url_func(validate_url_for)
  return Relation(
    Schema([dict(name='url',type="STRING")]),
    (
      (generate_url(key),)
      for key in bucket.list(prefix=prefix)
    )
  )

 
def http_url(conn, bucket_name, path=''):
  return conn.calling_format.build_url_base(
    conn, 
    conn.protocol,
    conn.server_name(conn.port),
    bucket_name,
    path
  )


def generate_anon_url(key, protocol):
  conn = key.bucket.connection
  return conn.calling_format.build_url_base(
    conn, 
    protocol,
    conn.server_name(conn.port),
    key.bucket.name, key.key
  )

def generate_url_func(validate_url_for, force_http=False):
  if validate_url_for is not None:
    return lambda key: key.generate_url(validate_url_for, force_http=force_http)
  else:
    protocol = 'http' if force_http else 'https'
    return lambda key: generate_anon_url(key, protocol)





