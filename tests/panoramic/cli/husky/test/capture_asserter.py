import enum
import os
import re
from pathlib import Path
from typing import Any, Callable

import schematics
import xxhash
import yaml

from tests.panoramic.cli.settings import WRITE_EXPECTATIONS


class BaseCaptureAsserter:
    def __init__(self, test_request, test_description_generator: Callable[[Any], str]):
        self.test_request = test_request
        self.test_description_generator = test_description_generator
        capture_dir = os.path.join(test_request.fspath.dirname, 'test_expectations')
        Path(capture_dir).mkdir(parents=True, exist_ok=True)
        file_name = '{}_{}'.format(
            re.sub('[^0-9a-zA-Z_]+', '', test_request.node.nodeid.replace('/', '_').replace(' ', '_'))[51:][:30],
            xxhash.xxh32(test_request.node.nodeid.encode('cp1252')).hexdigest(),
        )
        self.capture_file_path = os.path.join(capture_dir, f'{file_name}.yaml')

    def eval(self, test_input, actual_result: Any):
        """
        Runs the whole assert.
        Loads expectation from file
        Converts actual item to serializable primitive structure
        Asserts actual with expectation
        Saves the actual if a flag is set.
        """
        actual = self.item_to_primitive(actual_result)

        expected = dict()
        if os.path.exists(self.capture_file_path):
            with open(self.capture_file_path, "r") as f:
                expected = list(yaml.load_all(f))[1]
        try:
            self.assert_item(actual, expected)
        except (AssertionError, KeyError):
            if WRITE_EXPECTATIONS:
                self._write_expectations(test_input, actual)

    def assert_item(self, actual, expected):
        """
        You can implement your own asserter, to provide more granular assert messages, or other than equality comparison.
        """
        assert actual == expected, 'Results dont match'

    def _write_expectations(self, test_input, actual):
        with open(self.capture_file_path, 'w+') as f:
            yaml.dump_all([self.test_description_generator(test_input), actual], f, default_flow_style=False)

    def item_to_primitive(self, item):
        """
        Converts the item to primitive object, that can be written to yaml file easily.
        Override to implement support for your own specific complex objects.
        :param item:
        :return:
        """
        if isinstance(item, enum.Enum):
            return f'{item.__class__.__name__}.{str(item.name)}'
        elif isinstance(item, int):
            return item
        elif isinstance(item, str):
            return item
        elif isinstance(item, list):
            return [self.item_to_primitive(i) for i in item]
        elif isinstance(item, tuple):
            if item.__class__.__name__ == 'tuple':
                return [self.item_to_primitive(i) for i in item]
            else:
                # it is namedtuple
                return {k: self.item_to_primitive(v) for k, v in item._asdict().items()}
        elif isinstance(item, dict):
            return {k: self.item_to_primitive(v) for k, v in item.items()}
        elif isinstance(item, schematics.Model):
            return item.to_primitive()
        else:
            return str(item)
