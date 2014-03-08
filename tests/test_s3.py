from functools import partial
from StringIO import StringIO
from datetime import datetime

from nose.tools import *
from splicer.ast import *
from splicer.operations import query_zipper

from splicer_aws.s3_adapter import S3Adapter


def test_evaluate():
  adapter = S3Adapter(
    logs = dict(
      bucket = "aws-publicdatasets",
      anon = True,
      prefix = "/common-crawl/",
      pattern = "{timestamp}/{server}"
    )
  )

  relation = adapter.get_relation('logs')

  op = LoadOp('logs')
  loc = query_zipper(op).leftmost_descendant()
  
  res = adapter.evaluate(loc)

  import pdb; pdb.set_trace()

  eq_(
    res.root(),
    Function('s3_keys', Const(relation.bucket_name), Const(relation.prefix))
  )
  




