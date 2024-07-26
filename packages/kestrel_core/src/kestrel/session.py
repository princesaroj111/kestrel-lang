import logging
from contextlib import AbstractContextManager
from os import environ
from typing import Iterable
from uuid import uuid4

from kestrel.analytics import PythonAnalyticsInterface
from kestrel.cache import SqlCache
from kestrel.config import load_kestrel_config
from kestrel.config.internal import CACHE_INTERFACE_IDENTIFIER, VIRTUAL_CACHE_VAR_DATA
from kestrel.display import Display, GraphExplanation
from kestrel.exceptions import InstructionNotFound
from kestrel.frontend.completor import do_complete
from kestrel.frontend.parser import parse_kestrel_and_update_irgraph
from kestrel.interface import InterfaceManager
from kestrel.ir.graph import IRGraph
from kestrel.ir.instructions import DataSource, Explain, Instruction, Return
from typeguard import typechecked

_logger = logging.getLogger(__name__)


@typechecked
class Session(AbstractContextManager):
    """Kestrel huntflow execution session"""

    def __init__(self):
        self.session_id = uuid4()
        self.irgraph = IRGraph()
        self.config = load_kestrel_config()

        if "KESTREL_DEBUG" in environ:
            self.config["debug"] = True

        # load all interfaces; cache is a special interface
        cache = SqlCache(debug=self.config["debug"])

        # Python analytics are "built-in"
        pyanalytics = PythonAnalyticsInterface()

        self.interface_manager = InterfaceManager([cache, pyanalytics])

    def execute(self, huntflow_block: str) -> Iterable[Display]:
        """Execute a Kestrel huntflow block.

        Execute a Kestrel statement or multiple consecutive statements (a
        huntflow block) This method has the context of already executed
        huntflow blocks in this session, so all existing variables can be
        referred in the new huntflow block.

        Parameters:
            huntflow_block: the new huntflow block to be executed

        Returns:
            Evaluated result per Return instruction
        """
        return list(self.execute_to_generate(huntflow_block))

    def parse_and_update_graph(self, huntflow_block: str) -> Iterable[Return]:
        """Parse a Kestrel huntflow and update the IRGraph

        Parameters:
            huntflow_block: the new huntflow block to be executed

        Returns:
            Return instructions from the huntflow_block
        """
        # Transcational huntflow block parsing/updating. If failed, roll back
        # all things done for this huntflow/code block
        irgraph_snapshot = self.irgraph.copy()
        try:
            rets = parse_kestrel_and_update_irgraph(
                huntflow_block, self.irgraph, self.config["entity_identifier"]
            )
            for s in self.irgraph.get_nodes_by_type(DataSource):
                if not s.store:
                    itf = self.interface_manager[s.interface]
                    s.store = itf.get_storage_of_datasource(s.datasource)
        except Exception as e:
            self.irgraph = irgraph_snapshot
            raise e

        return rets

    def execute_to_generate(self, huntflow_block: str) -> Iterable[Display]:
        """Execute a Kestrel huntflow and put results in a generator.

        Parameters:
            huntflow_block: the new huntflow block to be executed

        Yields:
            Evaluated result per Return instruction
        """
        rets = self.parse_and_update_graph(huntflow_block)
        for ret in rets:
            yield self.evaluate_instruction(ret)

    def evaluate_instruction(self, ins: Instruction) -> Display:
        """Evaluate a single Instruction.

        Parameters:
            ins: the instruction to evaluate

        Returns:
            Evaluated result (Kestrel Display object)
        """
        if ins not in self.irgraph:
            raise InstructionNotFound(ins.to_dict())

        pred, _ = self.irgraph.get_trunk_n_branches(ins)
        is_explain = isinstance(pred, Explain)
        display = GraphExplanation([])

        _interface_manager = (
            self.interface_manager.copy_with_virtual_cache()
            if is_explain
            else self.interface_manager
        )
        _cache = _interface_manager[CACHE_INTERFACE_IDENTIFIER]

        # The current logic leads to caching results from non-cache and lastly
        # evaluate in cache.
        # TODO: may evaluate cache first, then push dependent variables to the
        # last interface to eval; this requires priority of interfaces
        while True:
            for g in self.irgraph.find_dependent_subgraphs_of_node(ins, _cache):
                interface = _interface_manager[g.interface]
                _logger.debug("eval: subgraph: %s", [i.instruction for i in g.nodes()])
                _logger.debug("eval: interface = %s", interface)
                for iid, _display in (
                    interface.explain_graph(g, _cache)
                    if is_explain
                    else interface.evaluate_graph(g, _cache)
                ).items():
                    if is_explain:
                        display.graphlets.append(_display)
                        _cache[iid] = VIRTUAL_CACHE_VAR_DATA
                    else:
                        display = _display
                        _cache[iid] = display
                    if iid == ins.id:
                        return display

    def do_complete(self, huntflow_block: str, cursor_pos: int) -> Iterable[str]:
        """Kestrel code auto-completion.

        Parameters:
            huntflow_block: Kestrel code
            cursor_pos: the position to start completion (index in ``huntflow_block``)

        Returns:
            A list of suggested strings to complete the code
        """
        return do_complete(
            huntflow_block,
            cursor_pos,
            self.interface_manager,
            [v.name for v in self.irgraph.get_variables()],
        )

    def close(self):
        """Explicitly close the session.

        This may be executed by a context manager or when the program exits.
        """
        # Note there are two conditions that trigger this function, so it is probably executed twice
        # Be careful to write the logic in this function to avoid deleting nonexist files/dirs
        if CACHE_INTERFACE_IDENTIFIER in self.interface_manager:
            self.interface_manager.del_cache()

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()
