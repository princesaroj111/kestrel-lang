# Golden SAML AD FS Mail Access

This directory contains an example Kestrel data model mapping for _Security Datasets_ Golden SAML dataset (https://securitydatasets.com/notebooks/compound/GoldenSAMLADFSMailAccess.html).

Tha dataset is in turn based on Microsoft's SimuLand Golden SAML Lab Guide (https://simulandlabs.com/labs/GoldenSAML/README.html)

## Setup

### Data Ingestion

```
kestrel-tool mkdb --db sqlite:///golden_saml.db --table WindowsEvents WindowsEvents.json
```

### Data Source Configuration

As an example, if using a datasource compatible with SQLAlchemy, add the connection info to your `sqlalchemy.yaml` file (e.g. `~/.config/kestrel/sqlalchemy.yaml`):

```
connections:
    goldensaml:
        url: sqlite:////home/user/datasources/golden_saml.db
        table_creation_permission: true
```

In the same file, add datasources for each table/file in the dataset:
```
datasources:
    WindowsEvents:
        connection: goldensaml
        table: WindowsEvents
        timestamp: TimeGenerated
        timestamp_format: "%Y-%m-%d %H:%M:%S.%fZ"
        data_model_map: "/home/user/.config/kestrel/GoldenSAML_WindowsEvents.yaml"
```

Copy the example data model maps from this directory into your Kestrel config directory (e.g. `~/.config/kestrel`)

