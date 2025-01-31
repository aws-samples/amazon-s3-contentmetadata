# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

import unittest
from unittest.mock import patch, ANY
from datetime import datetime
from extraction.LabelExtraction import LabelExtraction
from extraction.S3Event import S3Event, EventType


class TestLabelExtraction(unittest.TestCase):

    @patch('boto3.client')
    def testLabelGeneration(self, mock_client):
        mock_client.detect_labels.return_value = {
            'Labels': ['a', 'b', 'c']
        }
        extractor = LabelExtraction(mock_client, mock_client)
        event = S3Event(EventType.CREATED, datetime.now(), "foo", "bar.jpg", None, "123", "abc123", None)
        result = extractor.extract(event)
        self.assertDictEqual({"labels": ["a", "b", "c"]}, result)
        mock_client.detect_labels.assert_called_with(Image={
            'S3Object': {
                'Bucket': "foo",
                'Name': "bar.jpg"
        }}, MaxLabels=ANY)
