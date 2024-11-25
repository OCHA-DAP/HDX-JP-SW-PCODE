import logging
import logging.config

logging.config.fileConfig("logging.conf")

import datetime
from json import dumps
from os import getenv
from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars

from check_pcodes import get_global_pcodes, process_resource
from helper.facade import facade
from helper.util import do_nothing_for_ever

logger = logging.getLogger(__name__)


def listener_main(**ignore):
    """
    Function to run when p-code detector is run in listener mode. 
    Basically this waits for 'resource-created' OR 'resource-data-changed' events and runs the p-code checking logic.
    """

    # Connect to Redis
    event_bus = connect_to_hdx_event_bus_with_env_vars()

    configuration = Configuration.read()

    with temp_dir(folder="TempPCodeDetector") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_folder, "saved_data", temp_folder, save=False, use_saved=False
            )
            global_pcodes = get_global_pcodes(
                configuration["global_pcodes"],
                retriever,
            )

    def event_processor(event):
        start_time = datetime.datetime.now()
        with temp_dir(folder="TempPCodeDetector") as temp_folder:
            with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
                retriever = Retrieve(
                    downloader, temp_folder, "saved_data", temp_folder, save=False, use_saved=False
                )
                try:
                    logger.info(f"Received event: {dumps(event, ensure_ascii=False, indent=4)}")
                    dataset_id = event.get("dataset_id")
                    resource_id = event.get("resource_id")
                    if dataset_id and resource_id:
                        dataset = Dataset.read_from_hdx(dataset_id)
                        resource = Resource.read_from_hdx(resource_id)
                        process_resource(resource, dataset, global_pcodes, retriever, configuration)
                        end_time = datetime.datetime.now()
                        elapsed_time = end_time - start_time
                        logger.info(f"Finished processing resource {resource['name']}, {resource['id']} in {str(elapsed_time)}")
                    return True, "Success"
                except Exception as exc:
                    logger.error(f"Exception of type {type(exc).__name__} while processing dataset {dataset_id}: {str(exc)}")
                    return False, str(exc)

    event_bus.hdx_listen(event_processor, allowed_event_types=["resource-created", "resource-data-changed"], max_iterations=10_000)


def main(**ignore):

    configuration = Configuration.read()

    with temp_dir(folder="TempPCodeDetector") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_folder, "saved_data", temp_folder, save=False, use_saved=False
            )
            global_pcodes = get_global_pcodes(
                configuration["global_pcodes"],
                retriever,
            )
            datasets = Dataset.get_all_datasets(rows=1000)
            for dataset in datasets:
                logger.info(f"Processing dataset {dataset['name']}")
                resources = dataset.get_resources()
                for resource in resources:
                    pcoded = process_resource(
                        resource,
                        dataset,
                        global_pcodes,
                        retriever,
                        configuration,
                        update=False,
                        cleanup=True,
                    )
                    logger.info(f"{resource['name']}: {pcoded}")


if __name__ == "__main__":
    if getenv("WORKER_ENABLED") != "true" and getenv("LISTENER_MODE") == "true":
        do_nothing_for_ever()
    else:
        main_function = listener_main if getenv("LISTENER_MODE") == "true" else main
        facade(
            main_function,
            # hdx_site="feature", # passing HDX server via the env variable HDX_URL
            user_agent="PCodesDetector",
            hdx_read_only=False,
            preprefix="HDXINTERNAL",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
