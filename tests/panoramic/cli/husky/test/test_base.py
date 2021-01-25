import os
from os.path import dirname
from pathlib import Path
from unittest import TestCase as _TestCase

from panoramic.cli.husky.common.util import random_int
from tests.panoramic.cli.husky.test.util import TEST_GLOBAL_DATASET_COMPANY_IDS
from tests.panoramic.cli.settings import WRITE_EXPECTATIONS


class BaseTest(_TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._record_expectations = WRITE_EXPECTATIONS
        """
        Set to true if u want to record test expectations
        """

    @property
    def _record_test_dir_path(self) -> str:
        import inspect

        class_file_path = dirname(inspect.getfile(self.__class__))
        return os.path.join(class_file_path, 'test_expectations')

    def _get_record_test_expecations_path(self, expectations_name: str) -> str:
        full_test_id = self.id()
        short_test_id = '.'.join(full_test_id.split('.')[-2:])
        record_test_file_name = f'{short_test_id}#{expectations_name}'
        return os.path.join(self._record_test_dir_path, record_test_file_name)

    def _mkdir_record_test(self):
        Path(self._record_test_dir_path).mkdir(parents=True, exist_ok=True)

    def _write_record_test(self, record_test_abs_path: str, actual_data: any):
        pass

    def get_record_test(self, record_test_abs_path: str) -> any:
        pass

    def read_test_expectations(self, expectations_name: str) -> any:
        """
        Returns expectations for this test.
        """
        self._mkdir_record_test()
        file_path = self._get_record_test_expecations_path(expectations_name)
        with open(file_path, 'r') as myfile:
            data = myfile.read()
            return data

    def write_test_expectations(self, expectations_name: str, data: any):
        """
        If recording is enabled, saves the test expectations for this test and expectation name.
        """
        if self._record_expectations:
            self._mkdir_record_test()
            file_path = self._get_record_test_expecations_path(expectations_name)
            with open(file_path, 'w') as myfile:
                myfile.write(data)

    def _api_path(self):
        """
        Return basic diesel api path
        """
        return '/api/v1'


class IntegrationTestCase(BaseTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = random_int()
        self.another_project_id = random_int()
        self.company_id = random_int()
        TEST_GLOBAL_DATASET_COMPANY_IDS.append(self.company_id)  # Mark the company as global.
        self.project_slug = 'pslug'
        self.another_project_slug = 'ppslug'
