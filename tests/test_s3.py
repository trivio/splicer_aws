from splicer.ast import *
from splicer.operations import visit
from datetime import datetime

def test_s3():

  # select logs greater than may 31 2013
  op = SelectionOp(
    LoadOp('logs'), 
    GtOp(Var('timestamp'), datetime(2013,5,31))
  )



  # Selecting only the timestamp column should
  # not decode the payload
  ProjectionOp(op, Var('timestamp'))

  # Selecting columns inside the payload
  # should be handled by splicer as it'll be done
  # locally in memory by the machine
  ProjectionOp(op, Var('timestamp'), Var('host'))

  # No Support for joins

  JoinOp(
    LoadOp('customer_ids'),
    ProjectionOp(op, Var('timestamp'), Var('host'))
  )



