import logging
import logging.config

logging.config.fileConfig("logging.conf")

import datetime
import requests
from json import dumps
from os import getenv
from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars

from check_location import check_location, get_global_pcodes
from helper.facade import facade
from helper.ckan import patch_resource_with_pcode_value
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

    with temp_dir() as temp_f:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_f, "saved_data", temp_f, save=False, use_saved=False
            )
            global_pcodes, global_miscodes = get_global_pcodes(
                configuration["global_pcodes"],
                retriever,
            )

    def event_processor(event):
        start_time = datetime.datetime.now()
        with temp_dir(folder="TempLocationExploration") as temp_folder:
            try:
                logger.info(f"Received event: {dumps(event, ensure_ascii=False, indent=4)}")
                dataset_id = event.get("dataset_id")
                resource_id = event.get("resource_id")
                if dataset_id and resource_id:
                    dataset = Dataset.read_from_hdx(dataset_id)
                    locations = dataset.get_location_iso3s()
                    pcodes = [pcode for iso in global_pcodes for pcode in global_pcodes[iso] if iso in locations]
                    miscodes = [pcode for iso in global_miscodes for pcode in global_miscodes[iso] if iso in locations]
                    for resource in dataset.get_resources():
                        if resource["id"] != resource_id:
                            continue
                        _process_resource(resource, dataset, pcodes, miscodes, temp_folder, configuration)
                        end_time = datetime.datetime.now()
                        elapsed_time = end_time - start_time
                        logger.info(f"Finished processing resource {resource['name']}, {resource['id']} in {str(elapsed_time)}")
                return True, "Success"
            except Exception as exc:
                logger.error(f"Exception of type {type(exc).__name__} while processing dataset {dataset_id}: {str(exc)}")
                return False, str(exc)

    event_bus.hdx_listen(event_processor, allowed_event_types=["resource-created", "resource-data-changed"])


def main(**ignore):

    configuration = Configuration.read()

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            retriever = Retrieve(
                downloader, temp_folder, "saved_data", temp_folder, save=True, use_saved=False
            )
            global_pcodes, global_miscodes = get_global_pcodes(
                configuration["global_pcodes"],
                retriever,
            )
            datasets = Dataset.get_all_datasets(rows=100)
            for dataset in datasets:
                locations = dataset.get_location_iso3s()
                pcodes = [pcode for iso in global_pcodes for pcode in global_pcodes[iso] if iso in locations]
                miscodes = [pcode for iso in global_miscodes for pcode in global_miscodes[iso] if iso in locations]
                resources = dataset.get_resources()
                for resource in resources:
                    pcoded, miscoded = _process_resource(
                        resource,
                        dataset,
                        pcodes,
                        miscodes,
                        temp_folder,
                        configuration,
                        update=False,
                    )
                    logger.info(f"{resource['name']}: {pcoded}, {miscoded}")


def _process_resource(resource, dataset, pcodes, miscodes, temp_folder, configuration, update=True):
    pcoded = None
    miscoded = None

    if dataset.get_organization()["name"] in configuration["org_exceptions"]:
        pcoded = False

    if resource.get_file_type().lower() not in configuration["allowed_filetypes"]:
        pcoded = False

    if pcoded is None:
        size = resource["size"]
        if (size is None or size == 0) and resource["resource_type"] == "api":
            try:
                resource_info = requests.head(resource["url"])
                # if size cannot be determined, set to the limit set in configuration so the resource is excluded
                size = int(resource_info.headers.get("Content-Length", configuration["resource_size"]))
            except:
                size = configuration["resource_size"]

        if size >= configuration["resource_size"]:
            pcoded = False

    if pcoded is None:
        pcoded, mis_pcoded, error = check_location(resource, pcodes, miscodes, temp_folder)
        if mis_pcoded:
            logger.warning(f"{dataset['name']}: {resource['name']}: may be mis-pcoded")

        if error:
            logger.error(f"{dataset['name']}: {resource['name']}: {error}")

    if update:
        try:
            patch_resource_with_pcode_value(resource['id'], pcoded)
        except Exception:
            logger.exception(f"Could not update resource {resource['id']} in dataset {dataset['name']}")
            raise

    return pcoded, miscoded


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
