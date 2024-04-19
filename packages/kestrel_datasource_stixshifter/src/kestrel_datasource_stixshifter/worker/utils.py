import ssl
from typing import Optional, Union, List
from dataclasses import dataclass
from pandas import DataFrame
from stix_shifter.stix_transmission.stix_transmission import StixTransmission

STOP_SIGN = "STOP"


@dataclass
class WorkerLog:
    level: int
    log: str


# if success == True, data and offset is not None
# if success == False, log is not None
@dataclass
class TransmissionResult:
    worker: str
    success: bool
    data: Optional[List[dict]]
    offset: Optional[int]
    log: Optional[WorkerLog]


# if success == True, data is not None
# if success == False, log is not None
@dataclass
class TranslationResult:
    worker: str
    success: bool
    data: Union[None, dict, DataFrame]
    log: Optional[WorkerLog]


def disable_cert_verification_on_transmission(trans: StixTransmission):
    ot = trans.entry_point.transmission()

    # currently all the following attributes point to the same object
    # iterate through them in case stix-shifter code changes in the future
    for attr in [
        x
        for x in dir(ot)
        if x.startswith("_BaseEntryPoint__") and x.endswith("_connector")
    ]:
        c = getattr(ot, attr)
        c.api_client.client.ssl_context.check_hostname = False
        c.api_client.client.ssl_context.verify_mode = ssl.CERT_NONE
