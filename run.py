import logging
import os
import json

from os.path import join

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir

from check_location import check_location, get_global_pcodes

from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars

setup_logging(log_file='/var/log/listener.log', file_log_level="INFO")
logger = logging.getLogger(__name__)


def listener_main(**ignore):
    """
    Function to run when p-code detector is run in listener mode. 
    Basically this waits for 'resource-created' OR 'resource-data-changed' events and runs the p-code checking logic.
    """

    # Connect to Redis
    event_bus = connect_to_hdx_event_bus_with_env_vars()

    configuration = Configuration.read()

    with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
        global_pcodes, global_miscodes = get_global_pcodes(
            configuration["global_pcodes"],
            downloader,
        )

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        def event_processor(event):
            logger.info('Received event: ' + json.dumps(event, ensure_ascii=False, indent=4))
            dataset_id = event.get('dataset_id')
            if dataset_id:
                dataset = Dataset.read_from_hdx(dataset_id)
                _process_dataset(configuration, global_pcodes, global_miscodes, temp_folder, dataset)
            return True, 'Success'


        event_bus.hdx_listen(event_processor, allowed_event_types=['resource-created', 'resource-data-changed'])


def main(**ignore):

    configuration = Configuration.read()

    with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
        global_pcodes, global_miscodes = get_global_pcodes(
            configuration["global_pcodes"],
            downloader,
        )

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        datasets = Dataset.search_in_hdx(
            fq='cod_level:"cod-standard"'
        ) + Dataset.search_in_hdx(
            fq='cod_level:"cod-enhanced"'
        )

        for dataset in datasets:
            _process_dataset(configuration, global_pcodes, global_miscodes, temp_folder, dataset)

def _process_dataset(configuration, global_pcodes, global_miscodes, temp_folder, dataset):
    locations = dataset.get_location_iso3s()
    pcodes = [pcode for iso in global_pcodes for pcode in global_pcodes[iso] if iso in locations]
    miscodes = [pcode for iso in global_miscodes for pcode in global_miscodes[iso] if iso in locations]

    resources = dataset.get_resources()
    for resource in resources:
        if resource.get("p_coded") is not None:
            continue

        if dataset.get_organization()["name"] == "hot":
            resource["p_coded"] = False
            continue

        if resource.get_file_type() not in configuration["allowed_filetypes"]:
            resource["p_coded"] = False
            continue

        if resource["size"] and resource["size"] > configuration["resource_size"]:
            resource["p_coded"] = False
            continue

        pcoded, mis_pcoded, error = check_location(resource, pcodes, miscodes, temp_folder)
        if mis_pcoded:
            logger.warning(f"{dataset['name']}: {resource['name']}: may be mis-pcoded")

        if error:
            logger.error(f"{dataset['name']}: {resource['name']}: {error}")

        resource["p_coded"] = pcoded

    try:
        dataset.update_in_hdx(
                    hxl_update=False,
                    operation="patch",
                    batch_mode="KEEP_OLD",
                    skip_validation=True,
                    ignore_check=True,
                )
    except HDXError:
        logger.exception(f"Could not update {dataset['name']}")


if __name__ == "__main__":
    main_function = listener_main if os.getenv('LISTENER_MODE') == 'true' else main
    facade(
        main_function,
        # hdx_site="feature", # passing HDX server via the env variable HDX_URL
        user_agent="LocationExploration",
        hdx_read_only=False,
        preprefix="HDXINTERNAL",
        project_config_yaml=join("config", "project_configuration.yml"),
    )
