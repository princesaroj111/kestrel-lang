""" mkdb: turn JSON logs into SQLAlchemy DBs (e.g. sqlite3)"""

import json

import pandas as pd
import sqlalchemy
import typer


def _normalize_event(event: dict) -> dict:
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
    return event


def _jsonify_complex(df: pd.DataFrame) -> pd.DataFrame:
    """JSONify non-numerical columns that have non-string objects (e.g. lists)"""
    cols = [
        col
        for col in df.columns
        if df[col].dtype == "object" and df[col][df[col].apply(type) != str].any()
    ]
    for col in cols:
        df[col] = df[col].apply(json.dumps)
    # To drop instead, use: return df.drop(cols, axis='columns')
    return df


def _read_events(filename: str) -> pd.DataFrame:
    """Read JSON lines from `filename` and return a DataFrame"""
    events = []
    with open(filename, "r") as fp:
        for line in fp:
            event = json.loads(line)
            event = _normalize_event(event)
            events.append(event)
    return pd.json_normalize(events)


def mkdb(
    db: str = typer.Option("sqlite:///events.db", help="Database connection string"),
    table: str = typer.Option("events", help="Table name"),
    filename: str = typer.Argument(..., help="File with JSON lines"),
):
    df = _read_events(filename)
    df = _jsonify_complex(df)
    engine = sqlalchemy.create_engine(db)
    with engine.connect() as conn:
        df.to_sql(table, conn, index=False)
