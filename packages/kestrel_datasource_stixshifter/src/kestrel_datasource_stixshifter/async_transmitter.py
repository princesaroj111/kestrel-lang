import asyncio
from asyncio.log import logger
import logging
from multiprocessing import Queue
from typing import Optional
from stix_shifter.stix_transmission import stix_transmission
from kestrel_datasource_stixshifter.worker.utils import TransmissionResult, WorkerLog


class AsyncTransmitter:
    def __init__(
        self,
        connector_name: str,
        connection_dict: dict,
        configuration_dict: dict,
        retrieval_batch_size: int,
        query: str,
        output_queue: Queue,
        limit: Optional[int],
        cool_down_after_transmission: int,
        verify_cert: bool,
        custom_mappings = None,
    ):
        self.connector_name = connector_name
        self.connection_dict = connection_dict
        self.configuration_dict = configuration_dict
        self.retrieval_batch_size = retrieval_batch_size
        self.query = query
        self.queue = output_queue
        self.limit = limit
        self.cool_down_after_transmission = cool_down_after_transmission
        self.verify_cert = verify_cert
        self.custom_mappings = custom_mappings
    async def run(self):
        logger.info(f"Transmitter worker started")
        self.worker_name = "Transmitter"
        self.transmission = stix_transmission.StixTransmission(
            self.connector_name,
            self.connection_dict,
            self.configuration_dict,
            self.custom_mappings
        )
        search_meta_result = await self.transmission.query_async(self.query)

        if search_meta_result["success"]:
            self.search_id = search_meta_result["search_id"]
            if await self.wait_datasource_search():
                await self.retrieve_data()
        else:

            err_msg = search_meta_result.get("error", "details not available")
            logger.error(err_msg)
            packet = TransmissionResult(
                self.worker_name,
                False,
                None,
                None,
                WorkerLog(
                    logging.ERROR,
                    f"STIX-shifter transmission.query() failed: {err_msg}",
                ),
            )
            await self.put_in_queue(packet)

    async def wait_datasource_search(self):
        status = {"success": True, "progress": 0, "status": "KINIT"}

        while status["success"] and status["status"] in ("KINIT", "RUNNING"):
            if status["status"] == "RUNNING":
                await asyncio.sleep(5)
            status = await self.transmission.status_async(self.search_id)
            if not status["success"]:
                err_msg = status.get("error", "details not available")
                packet = TransmissionResult(
                    self.worker_name,
                    False,
                    None,
                    None,
                    WorkerLog(
                        logging.ERROR,
                        f"STIX-shifter transmission.status() failed: {err_msg}",
                    ),
                )
                await self.put_in_queue(packet)
                return False
        return True

    async def retrieve_data(self):
        result_retrieval_offset = 0
        has_remaining_results = True
        metadata = None
        is_retry_cycle = False
        batch_size = self.retrieval_batch_size
        if self.limit and self.limit < self.retrieval_batch_size:
            batch_size = self.limit

        while has_remaining_results:
            packet = None
            print("search_id :", self.search_id)
            result_batch = await self.transmission.results_async(
                self.search_id,
                result_retrieval_offset,
                batch_size,
                metadata,
            )

            if result_batch["success"]:
                if result_batch["data"]:
                    packet = TransmissionResult(
                        self.worker_name,
                        True,
                        result_batch["data"],
                        result_retrieval_offset,
                        None,
                    )

                    # Prepare for next retrieval
                    result_retrieval_offset += len(result_batch["data"])
                    if "metadata" in result_batch:
                        metadata = result_batch["metadata"]

                    if self.limit:
                        if result_retrieval_offset >= self.limit:
                            has_remaining_results = False
                        else:
                            batch_size = self.limit - result_retrieval_offset
                            if batch_size > self.retrieval_batch_size:
                                batch_size = self.retrieval_batch_size
                else:
                    has_remaining_results = False

                is_retry_cycle = False

            else:
                err_msg = result_batch.get("error", "details not available")

                if (
                    err_msg.startswith(
                        f"{self.connector_name} connector error => server timeout_error"
                    )
                    and not is_retry_cycle
                ):
                    packet = TransmissionResult(
                        self.worker_name,
                        False,
                        None,
                        None,
                        WorkerLog(
                            logging.INFO,
                            "Busy CPU; hit stix-shifter aiohttp connection timeout; retry.",
                        ),
                    )
                    is_retry_cycle = True

                else:
                    packet = TransmissionResult(
                        self.worker_name,
                        False,
                        None,
                        None,
                        WorkerLog(
                            logging.ERROR,
                            f"STIX-shifter transmission.results() failed: {err_msg}",
                        ),
                    )
                    has_remaining_results = False

            if packet:
                await self.put_in_queue(packet)

        # Cool down after transmission if needed
        if self.cool_down_after_transmission:
            await asyncio.sleep(self.cool_down_after_transmission)

    async def put_in_queue(self, packet):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.queue.put, packet)
