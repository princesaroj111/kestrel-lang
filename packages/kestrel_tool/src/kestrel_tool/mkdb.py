""" mkdb: turn JSON logs into SQLAlchemy DBs (e.g. sqlite3)"""

import json
import re

import pandas as pd
import sqlalchemy
import typer

RE_UUID = re.compile(
    "^%?{?([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})}?$"
)


def _normalize_event(event: dict) -> dict:
    # We could definitely optimize this
    if "tags" in event:
        # Blue Team Village CTF data (cribl?)
        del event["tags"]
    if "process" in event:
        # SecurityDatasets.com GoldenSAML WindowsEvents: case inconsistency?
        event["Process"] = event["process"]
        del event["process"]
    if "ProcessID" in event:
        # The case of the final 'd' seems to vary for Windows events!
        event["ProcessId"] = event["ProcessID"]
        del event["ProcessID"]
    if "NewProcessId" in event:
        event["ParentProcessId"] = event["ProcessId"]
        event["ProcessId"] = event["NewProcessId"]
        del event["NewProcessId"]
        event["ParentImage"] = event["ParentProcessName"]
        del event["ParentProcessName"]
        event["Image"] = event["NewProcessName"]
        del event["NewProcessName"]
    if "IpPort" in event:
        # normalize `IpPort` in auth log into generic source port
        event["SourcePort"] = event["IpPort"]
        del event["IpPort"]
    if "IpAddress" in event:
        # normalize `IpAddress` in auth log into generic source address
        event["SourceAddress"] = event["IpAddress"]
        del event["IpAddress"]

    for k, v in event.items():
        if isinstance(v, str):
            try:
                event[k] = json.loads(v)
            except json.JSONDecodeError:
                pass  # maybe it's NOT JSON

    for k in list(event):
        if k.endswith("_string"):
            base_key = k[:-7]
            if base_key not in event or not event[base_key]:
                event[base_key] = event[k]
                del event[k]
        if k.endswith("_long"):
            base_key = k[:-5]
            if base_key not in event or not event[base_key]:
                event[base_key] = int(event[k])
                del event[k]

    return event


def _read_events(filename: str) -> pd.DataFrame:
    """Read JSON lines from `filename` and return a DataFrame"""
    events = []
    with open(filename, "r") as fp:
        for line in fp:
            event = json.loads(line)
            event = _normalize_event(event)
            events.append(event)
    return pd.json_normalize(events)


def _update_cell(value):
    """Replace a cell value"""
    # dump list/dict
    if isinstance(value, (list, dict)):
        return json.dumps(value)

    # extract UUID
    if isinstance(value, str):
        matched = RE_UUID.match(value)
        if matched:
            return matched.group(1)

    # do nothing
    return value


def _integerize_columns(df):
    for col in df.columns:
        try:
            df[col] = df[col].astype(pd.Int64Dtype())
        except:
            pass


def mkdb(
    db: str = typer.Option("sqlite:///events.db", help="Database connection string"),
    table: str = typer.Option("events", help="Table name"),
    filename: str = typer.Argument(..., help="File with JSON lines"),
):
    # basic normalize to DataFrame
    df = _read_events(filename)

    # post-processing values
    df = df.map(_update_cell)

    # convert values to integer if possible
    _integerize_columns(df)

    # write to db
    engine = sqlalchemy.create_engine(db)
    with engine.connect() as conn:
        df.to_sql(table, conn, index=False)
