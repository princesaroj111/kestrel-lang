import base64
import tempfile
from math import ceil, sqrt
from typing import Iterable, Mapping

import matplotlib.pyplot as plt
import networkx as nx
import numpy
import sqlparse
from kestrel.display import AnalyticOperation, Display, GraphExplanation, NativeQuery
from kestrel.ir.graph import IRGraph
from kestrel.ir.instructions import Construct, DataSource, Instruction, Variable
from pandas import DataFrame
from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import guess_lexer
from pygments.lexers.kusto import KustoLexer
from pygments.lexers.sql import SqlLexer


def gen_label_mapping(g: IRGraph) -> Mapping[Instruction, str]:
    d = {}
    for n in g:
        if isinstance(n, Variable):
            d[n] = n.name
        elif isinstance(n, Construct):
            d[n] = n.id.hex[:4]
        elif isinstance(n, DataSource):
            d[n] = n.datasource
        else:
            d[n] = f"[{n.instruction.upper()}]"
    return d


def to_html_blocks(d: Display) -> Iterable[str]:
    if isinstance(d, DataFrame):
        d = d.replace("", numpy.nan).dropna(axis="columns", how="all")
        escaped_df = d.map(lambda x: x.replace("$", "\\$") if isinstance(x, str) else x)
        yield escaped_df.to_html(index=False, na_rep="")
    elif isinstance(d, GraphExplanation):
        for graphlet in d.graphlets:
            graph = IRGraph(graphlet.graph)
            fig_side_length = min(10, ceil(sqrt(len(graph))) + 1)
            plt.figure(figsize=(fig_side_length, fig_side_length))
            nx.draw(
                graph,
                with_labels=True,
                labels=gen_label_mapping(graph),
                font_size=8,
                node_size=260,
                node_color="#bfdff5",
            )
            with tempfile.NamedTemporaryFile(delete_on_close=False) as tf:
                tf.close()
                plt.savefig(tf.name, format="png")
                with open(tf.name, "rb") as tfx:
                    data = tfx.read()

            img = data_uri = base64.b64encode(data).decode("utf-8")
            imgx = f'<img src="data:image/png;base64,{img}">'
            yield imgx

            if isinstance(graphlet.action, NativeQuery):
                native_query = graphlet.action
                language = native_query.language
                query = native_query.statement
                if language == "SQL":
                    lexer = SqlLexer()
                    query = sqlparse.format(query, reindent=True, keyword_case="upper")
                elif language == "KQL":
                    lexer = KustoLexer()
                else:
                    lexer = guess_lexer(query)
                query = highlight(query, lexer, HtmlFormatter())
                style = "<style>" + HtmlFormatter().get_style_defs() + "</style>"
                yield style + query
            elif isinstance(graphlet.action, AnalyticOperation):
                analytic_operation = graphlet.action
                data = {
                    "": ["Interface", "Operation"],
                    "Value": [
                        analytic_operation.interface,
                        analytic_operation.operation,
                    ],
                }
                yield DataFrame(data).to_html(index=False)
