import csv
import logging
import warnings
from shapely.errors import ShapelyDeprecationWarning

from hdx.data.dataset import Dataset
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir

from check_location import check_location

setup_logging()
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

lookup = "hdx-location-exploration"


def main(**ignore):

    with temp_dir(folder="TempLocationExploration") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:
            datasets = Dataset.search_in_hdx(
                fq='vocab_Topics:"common operational dataset - cod"'
            )
            logger.info(f"Found {len(datasets)} datasets")

            with open("datasets_location_status.csv", "w") as c:
                writer = csv.writer(c)
                writer.writerow(["dataset name", "dataset title", "pcoded", "latlong", "error"])

                for dataset in datasets:
                    pcoded, latlong, error = check_location(dataset, temp_folder)

                    writer.writerow([dataset["name"], dataset["title"], pcoded, latlong, error])


if __name__ == "__main__":
    facade(
        main,
        hdx_site="prod",
        user_agent="LocationExploration",
        hdx_read_only=True,
        preprefix="HDXINTERNAL",
    )
