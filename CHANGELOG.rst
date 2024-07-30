=========
Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_.

Unreleased
==========

This is the Changelog for Kestrel 2. Look for Changelog for Kestrel 1 in the ``develop_v1`` branch.

2.0.0b (2024-07-30)
==================

Added
-----

- Commands supported
    - NEW
    - GET
    - FIND
    - DISP
    - INFO
    - APPLY
    - EXPLAIN
    - expression

- Supported Entities
    - `event` is a first-class citizen in Kestrel v2
    - Check `kestrel.mapping.types.*` for details

- Supported Relations
    - Relation between entity and entity
    - Relation between event and entity
    - Check `kestrel.config.relations.*` for details

- Kestrel Intermediate Representation Graph (IRGraph)
    - GIT compilation with IRGraph
    - Kestrel segments IRGraph to execute on multiple interfaces/datastores/exec_env
    - Kestrel cache glues executions together for a session

- OCSF/ECS/STIX syntax supported in frontend
    - Type inferencing supported
    - Comparison field translation supported
    - Project field translation supported

- Datasource Interfaces
    - Sqlalchemy fully working
        - Multi-store support
        - Query column translation supported
        - Value translation supported
    - Opensearch halfy done

- Analytics Interfaces
    - Python analytics interface works for `DataFrame` but not `Display` objects

- Kestrel Tool
    - `mkdb` to ingest NLJSON logs into SQL databases

- Example Mappings
    - Four examples mappings created for BlackHat 2024 (SecurityDatasets GoldenSAML case)

.. _Keep a Changelog: https://keepachangelog.com/en/1.0.0/
