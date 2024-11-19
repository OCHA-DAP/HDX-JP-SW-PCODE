from json import load
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from check_pcodes import get_global_pcodes, process_resource


class TestCheckPcodes:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="feature",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
            ]
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def dataset(self):
        class Dataset:
            @staticmethod
            def read_from_hdx(dataset_name):
                return Dataset.load_from_json(join("fixtures", "input", f"{dataset_name}.json"))

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_get_global_pcodes(self, configuration, fixtures, input_folder):
        with temp_dir(folder="TestPcodeDetector") as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                global_pcodes = get_global_pcodes(
                    configuration["global_pcodes"],
                    retriever,
                    locations=["AFG", "COL"],
                )
                assert global_pcodes == load(open(join(fixtures, "afg_col_pcodes.txt")))

    def test_process_resource(self, configuration, fixtures, input_folder):
        dataset = Dataset.load_from_json(join(input_folder, "test-data-for-p-code-detector.json"))
        resources = dataset.get_resources()
        codes = [[False, True], [True, None], [False, True], [True, None], [False, None]]
        with temp_dir(folder="TestPcodeDetector") as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                global_pcodes = get_global_pcodes(
                    configuration["global_pcodes"],
                    retriever,
                )

                for i, resource in enumerate(resources):
                    pcoded = process_resource(
                        resource,
                        dataset,
                        global_pcodes,
                        retriever,
                        configuration,
                        update=False,
                        cleanup=False,
                    )
                    assert pcoded == codes[i][0]
