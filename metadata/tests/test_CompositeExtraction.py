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

from datetime import datetime
from unittest import TestCase
from unittest.mock import Mock

from extraction.AbstractMetadataExtractor import AbstractMetadataExtractor
from extraction.CompositeExtraction import CompositeExtraction
from extraction.S3Event import S3Event, EventType


class TestCompositeExtraction(TestCase):

    def testCompositeExtraction(self):
        extractor1 = Mock(spec=AbstractMetadataExtractor)
        extractor1.applies_to.return_value = True
        extractor1.extract.return_value = {'thing':'thang'}

        extractor2 = Mock(spec=AbstractMetadataExtractor)
        extractor2.applies_to.return_value = False

        event = S3Event(EventType.CREATED, datetime.now(), "foo", "bar.jpg", None, "123", "abc123", None)
        uut = CompositeExtraction(None, [extractor1, extractor2])
        result = uut.extract(event)

        extractor1.applies_to.assert_called()
        extractor1.extract.assert_called()
        extractor2.applies_to.assert_called()
        extractor2.extract.assert_not_called()
        self.assertEqual(result, {'thing':'thang'})
