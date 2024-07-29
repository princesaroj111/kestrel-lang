class KestrelError(Exception):
    pass


class InstructionNotFound(KestrelError):
    pass


class InvalidInstruction(KestrelError):
    pass


class InvalidComparison(KestrelError):
    pass


class MismatchedFieldValueInMultiColumnComparison(KestrelError):
    pass


class InvalidOperatorInMultiColumnComparison(KestrelError):
    pass


class InvalidSeralizedGraph(KestrelError):
    pass


class InvalidSeralizedInstruction(KestrelError):
    pass


class InvalidDataSource(KestrelError):
    pass


class InvalidYamlInConfig(KestrelError):
    pass


class InvalidKestrelConfig(KestrelError):
    pass


class MissingEntityIdentifierInConfig(KestrelError):
    pass


class InvalidKestrelRelationTable(KestrelError):
    pass


class UnsupportedObjectRelation(KestrelError):
    pass


class DuplicatedRelationMapping(KestrelError):
    pass


class VariableNotFound(KestrelError):
    pass


class SourceNotFound(KestrelError):
    pass


class ReferenceNotFound(KestrelError):
    pass


class DataSourceNotFound(KestrelError):
    pass


class DuplicatedVariable(KestrelError):
    pass


class DuplicatedReference(KestrelError):
    pass


class DuplicatedDataSource(KestrelError):
    pass


class DuplicatedSingletonInstruction(KestrelError):
    pass


class MultiInterfacesInGraph(KestrelError):
    pass


class MultiSourcesInGraph(KestrelError):
    pass


class LargerThanOneIndegreeInstruction(KestrelError):
    pass


class DanglingReferenceInFilter(KestrelError):
    pass


class DanglingFilter(KestrelError):
    pass


class DuplicatedReferenceInFilter(KestrelError):
    pass


class MissingReferenceInFilter(KestrelError):
    pass


class InvalidSerializedDatasourceInterfaceCacheCatalog(KestrelError):
    pass


class InevaluableInstruction(KestrelError):
    pass


class MappingParseError(KestrelError):
    pass


class InterfaceNotFound(KestrelError):
    pass


class IRGraphMissingNode(KestrelError):
    pass


class InterfaceNotConfigured(KestrelError):
    pass


class InvalidInterfaceImplementation(KestrelError):
    pass


class ConflictingInterfaceScheme(KestrelError):
    pass


class DataSourceError(KestrelError):
    pass


class UnsupportedOperatorError(KestrelError):
    """The data source doesn't support this operator"""

    pass


class IncompleteDataMapping(KestrelError):
    pass


class InvalidAnalytics(KestrelError):
    pass


class InvalidAnalyticsArgumentCount(KestrelError):
    pass


class InvalidAnalyticsInterfaceImplementation(KestrelError):
    pass


class InvalidAnalyticsOutput(KestrelError):
    pass


class AnalyticsError(KestrelError):
    pass


class SourceSchemaNotFound(KestrelError):
    pass


class InvalidProjectEntityFromEntity(KestrelError):
    pass


class EntityNotFound(KestrelError):
    pass


class InvalidMappingWithMultipleIdentifierFields(KestrelError):
    pass


class InvalidTransformerInMapping(KestrelError):
    pass


class InvalidAttributes(KestrelError):
    pass
