"""Python analytics interface executes Python function as Kestrel analytics.

Use a Python Analytics
----------------------

Create a profile for each analytics in the python analytics interface config
file (YAML):

- Default path: ``~/.config/kestrel/pythonanalytics.yaml``.
- A customized path specified in the environment variable ``KESTREL_PYTHON_ANALYTICS_CONFIG``.

Example of the python analytics interface config file:

.. code-block:: yaml

    profiles:
        analytics-name-1: # the analytics name to use in the APPLY command
            module: /home/user/kestrel-analytics/analytics/piniponmap/analytics.py
            func: analytics # the analytics function in the module to call
        analytics-name-2:
            module: /home/user/kestrel-analytics/analytics/suspiciousscoring/analytics.py
            func: analytics

Develop a Python Analytics
--------------------------

A Python analytics is a python function that follows the rules:

#. The function takes in one or more Kestrel variable dumps in Pandas DataFrames.

#. The return of the function is a tuple containing either or both:

    - Updated variables. The number of variables can be either 0, e.g.,
      visualization analytics, or the same number as input Kestrel variables.
      The order of the updated variables should follow the same order as input
      variables.

    - An object to display, which can be any of the following types:

        - Kestrel display object

        - HTML element as a string

        - Matplotlib figure (by default, Pandas DataFrame plots use this)

   The display object can be either before or after updated variables. In other
   words, if the input variables are ``var1``, ``var2``, and ``var3``, the
   return of the analytics can be either of the following:

   .. code-block:: python

       # the analytics enriches variables without returning a display object
       return var1_updated, var3_updated, var3_updated

       # this is a visualization analytics and no variable updates
       return display_obj

       # the analytics does both variable updates and visualization
       return var1_updated, var3_updated, var3_updated, display_obj

       # the analytics does both variable updates and visualization
       return display_obj, var1_updated, var3_updated, var3_updated


#. Parameters in the APPLY command are passed in as keyword args when possible,
   otherwise as environment variables.  The interface will inspect the signature
   of the analytics function to determine which methods to use.  For example, the
   following analytics function would be called with keyword args taken exactly
   from the ``WITH`` part of the Kestrel statement.

   .. code-block::

   def my_analytic(df: pd.DataFrame, x: int = 0, y: float = 0.5)

   This function could be called as:

   .. code-block::

       APPLY python://my_analytic ON var1 WITH x=1, y=0.7

   The names of the environment variables are the exact parameter keys given in the
   ``APPLY`` command. For example, the following command

   .. code-block::

       APPLY python://a1 ON var1 WITH XPARAM=src_ref.value, YPARAM=number_observed

   creates environment variables ``$XPARAM`` with value ``src_ref.value`` and
   ``$YPARAM`` with value ``number_observed`` to be used by the analytics
   ``a1``. After the execution of the analytics, the environment variables will
   be roll back to the original state.

#. The Python function could spawn other processes or execute other binaries,
   where the Python function just acts like a wrapper. Check our `domain name
   lookup analytics`_ as an example.

.. _domain name lookup analytics: https://github.com/opencybersecurityalliance/kestrel-analytics/tree/release/analytics/domainnamelookup

"""

import inspect
import json
import logging
import os
import pathlib
import sys
import traceback
from contextlib import AbstractContextManager
from importlib.util import module_from_spec, spec_from_file_location
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional
from uuid import UUID

from kestrel.analytics.config import get_profile, load_profiles
from kestrel.display import AnalyticOperation, GraphletExplanation
from kestrel.exceptions import (
    AnalyticsError,
    InvalidAnalytics,
    InvalidAnalyticsArgumentCount,
    InvalidAnalyticsInterfaceImplementation,
    InvalidAnalyticsOutput,
)
from kestrel.interface import AnalyticsInterface
from kestrel.ir.graph import IRGraphEvaluable
from kestrel.ir.instructions import (
    Analytic,
    Instruction,
    TransformingInstruction,
    Variable,
)
from pandas import DataFrame

_logger = logging.getLogger(__name__)


class PythonAnalyticsJob:
    """Simple config class to hold all the bits necessary to call the external analytics"""

    def __init__(
        self,
        iid: UUID,
        cache: MutableMapping[UUID, Any],
    ):
        self.cache = cache
        self.input_iid = iid
        self.output_iid: Optional[UUID] = None
        self.analytic: str = ""
        self.params: dict = {}

    def run(self, config: dict) -> DataFrame:
        module_name, func_name = get_profile(self.analytic, config)
        df = self.cache[self.input_iid]
        with PythonAnalytics(
            self.analytic, module_name, func_name, self.params
        ) as func:
            df = func(df)
        _logger.debug("python analytics job result:\n%s", df)
        return df

    def get_module_and_func_name(self, config: dict) -> str:
        module_name, func_name = get_profile(self.analytic, config)
        return module_name, func_name


class PythonAnalyticsInterface(AnalyticsInterface):
    def __init__(
        self,
        serialized_cache_catalog: Optional[str] = None,
        session_id: Optional[UUID] = None,
    ):
        _logger.debug("PythonAnalyticsInterface: loading config")
        super().__init__(serialized_cache_catalog, session_id)
        self.config = load_profiles()

    @staticmethod
    def schemes() -> Iterable[str]:
        return ["python"]

    def get_datasources(self) -> List[str]:
        return list(self.config)

    def get_storage_of_datasource(self, datasource: str) -> str:
        return "local"

    def store(
        self,
        instruction_id: UUID,
        data: DataFrame,
    ):
        raise NotImplementedError("PythonAnalyticsInterface.store")  # TEMP

    def explain_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instructions_to_explain: Optional[Iterable[Instruction]] = None,
    ) -> Mapping[UUID, GraphletExplanation]:
        mapping = {}
        if not instructions_to_explain:
            instructions_to_explain = graph.get_sink_nodes()
        for instruction in instructions_to_explain:
            dep_graph = graph.duplicate_dependent_subgraph_of_node(instruction)
            graph_dict = dep_graph.to_dict()
            job = self._evaluate_instruction_in_graph(graph, cache, instruction)
            module_name, func_name = job.get_module_and_func_name(self.config)
            action = AnalyticOperation("Python", module_name + "::" + func_name)
            mapping[instruction.id] = GraphletExplanation(graph_dict, action)
        return mapping

    def evaluate_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instructions_to_evaluate: Optional[Iterable[Instruction]] = None,
    ) -> Mapping[UUID, DataFrame]:
        _logger.debug(
            "python: graph: %s", json.dumps(json.loads(graph.to_json()), indent=4)
        )
        mapping = {}
        if not instructions_to_evaluate:
            instructions_to_evaluate = graph.get_sink_nodes()
        for instruction in instructions_to_evaluate:
            _logger.debug("python: inst to evaluate: %s", instruction)
            job = self._evaluate_instruction_in_graph(graph, cache, instruction)
            mapping[job.output_iid] = job.run(self.config)
        return mapping

    def _evaluate_instruction_in_graph(
        self,
        graph: IRGraphEvaluable,
        cache: MutableMapping[UUID, Any],
        instruction: Instruction,
    ) -> PythonAnalyticsJob:
        _logger.debug("python analytics: instruction = %s", instruction)

        if isinstance(instruction, TransformingInstruction):
            if instruction.id in cache:
                iid = instruction.id
                _logger.debug("python analytics: got iid = %s from cache", iid)
                job = PythonAnalyticsJob(iid, cache)
            else:
                trunk, _ = graph.get_trunk_n_branches(instruction)
                job = self._evaluate_instruction_in_graph(graph, cache, trunk)

            if isinstance(instruction, Analytic):
                job.analytic = instruction.name
                job.params = instruction.params
                _logger.debug("python analytics: %s", job.analytic)
            elif isinstance(instruction, Variable):
                if not job.input_iid:
                    job.input_iid = instruction.id
                    _logger.debug("python analytics: input_iid = %s", job.input_iid)
                else:
                    job.output_iid = instruction.id
                    _logger.debug("python analytics: output_iid = %s", job.output_iid)

        return job


class PythonAnalytics(AbstractContextManager):
    def __init__(
        self, analytic_name: str, module_name: str, func_name: str, parameters: dict
    ):
        self.name = analytic_name
        self.func_name = func_name
        self.parameters = parameters
        self.module_path = pathlib.Path(module_name).expanduser().resolve()
        self.module_path_dir_str = str(self.module_path.parent)
        self.use_env = False

    def __enter__(self):
        # accommodate any other Python modules to load in the dir
        self.syspath = sys.path.copy()
        sys.path.append(self.module_path_dir_str)

        # accommodate any other executables or data to load in the dir
        self.cwd_original = os.getcwd()
        os.chdir(self.module_path_dir_str)

        # time to load the analytics function
        self.analytics_function = self._locate_analytics_func(self._load_module())

        # inspect signature of function and skip env vars if it has kwargs
        self.use_env = self._set_env_vars()

        # passing parameters as environment variables
        if self.use_env:
            self.environ_original = os.environ.copy()
            if self.parameters:
                if isinstance(self.parameters, Mapping):
                    parameters = {k: str(v) for k, v in self.parameters.items()}
                    _logger.debug(f"setting parameters as env vars: {parameters}")
                    os.environ.update(parameters)
                else:
                    raise InvalidAnalyticsInterfaceImplementation(
                        "parameters should be passed in as a Mapping"
                    )

        return self._execute

    def __exit__(self, exception_type, exception_value, _traceback):
        sys.path = self.syspath
        os.chdir(self.cwd_original)
        if self.use_env:
            os.environ = self.environ_original

    def _execute(self, dataframe: DataFrame):
        """Execute the analytics

        Args:
            dataframe (DataFrame): input variable to the analytics.

        Returns:

            DataFrame: the analytics output (i.e. "enriched" DataFrame)

        """
        input_dataframes = [dataframe]  # TEMP
        if len(input_dataframes) != self._get_var_count():
            raise InvalidAnalyticsArgumentCount(
                self.name, len(input_dataframes), self._get_var_count()
            )

        try:
            if self.use_env:
                outputs = self.analytics_function(*input_dataframes)
            else:
                outputs = self.analytics_function(*input_dataframes, **self.parameters)
        except Exception as e:
            _logger.error('"%s" failed at execution: %s', self.name, e, exc_info=e)
            raise AnalyticsError(f"{self.name} failed at execution") from e

        if not isinstance(outputs, tuple):
            outputs = (outputs,)

        output_dfs, output_dsps = [], []
        for x in outputs:
            x_class_str = type(x).__module__ + "." + type(x).__name__
            if isinstance(x, DataFrame):
                output_dfs.append(x)
            elif isinstance(x, str):
                _logger.info(
                    f'analytics "{self.name}" yielded a string return. treat it as an HTML element.'
                )
                output_dsps.append(x)  # FIXME:DisplayHtml(x))
            elif x_class_str == "matplotlib.figure.Figure":
                _logger.info(f'analytics "{self.name}" yielded a figure.')
                output_dsps.append(x)  # FIXME:DisplayFigure(x))
            else:
                raise InvalidAnalyticsOutput(self.name, type(x))

        if not outputs:
            raise AnalyticsError(f'analytics "{self.name}" yield nothing')
        if len(output_dsps) > 1:
            raise AnalyticsError(
                f'analytics "{self.name}" yielded more than one Kestrel Display object'
            )
        if output_dfs:
            if len(output_dfs) != len(input_dataframes):
                raise AnalyticsError(
                    f'analytics "{self.name}" yielded less/more Kestrel variable(s) than given'
                )
            return output_dfs[0]  # TEMP

        display = output_dsps[0] if output_dsps else None
        return display

    def _load_module(self):
        spec = spec_from_file_location(
            "kestrel_analytics_python.analytics.{profile_name}", str(self.module_path)
        )

        try:
            module = module_from_spec(spec)
            spec.loader.exec_module(module)
        except ModuleNotFoundError as e:
            raise AnalyticsError(
                f"{self.name} misses dependent library: {e.name}",
                "pip install the corresponding Python package",
            )
        except Exception as e:
            if isinstance(e, AttributeError) and e.args == (
                "'NoneType' object has no attribute 'loader'",
            ):
                raise AnalyticsError(
                    f"{self.name} is not found",
                    "please make sure the Python module and function specified in the profile (configuration) exist",
                )
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                error = "".join(
                    traceback.format_exception(exc_type, exc_value, exc_traceback)
                )
                raise AnalyticsError(f"{self.name} failed at importing:\n{error}")

        return module

    def _locate_analytics_func(self, module):
        if hasattr(module, self.func_name):
            return getattr(module, self.func_name)
        raise InvalidAnalytics(
            self.name,
            "python",
            f'function "{self.func_name}" not exist in module: {self.module_path}',
        )

    def _get_var_count(self):
        """Determine number of vars/DataFrames the analytics func expects"""
        sig = inspect.signature(self.analytics_function)
        # Count of params with type DataFrame
        # If there are no type annotations, then fall back to param count
        df_count = sum(1 for i in sig.parameters.values() if i.annotation == DataFrame)
        return df_count if df_count else len(sig.parameters)

    def _set_env_vars(self):
        """Check if the analytics function DOES NOT accept any non-DataFrame parameters. If so, return True and use env to pass params."""
        sig = inspect.signature(self.analytics_function)
        return (
            sum(1 for i in sig.parameters.values() if i.annotation != DataFrame) == 0
            or len(sig.parameters) == 1
        )
