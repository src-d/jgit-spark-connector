from __future__ import absolute_import
from importlib import import_module
from bblfsh.sdkversion import VERSION

from sourced.engine.engine import Engine, SourcedDataFrame

def parse_uast_node(data):
    """
    Parses a byte array and turns it into an UAST node.

    >>> parse_uast_node(row["uast"])

    :param data: binary-encoded uast as a byte array
    :type data: byte array
    :rtype: UAST node
    """
    return import_module(
        "bblfsh.gopkg.in.bblfsh.sdk.%s.uast.generated_pb2" % VERSION)\
        .Node.FromString(data)