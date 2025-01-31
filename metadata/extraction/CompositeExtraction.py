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

import logging

from extraction.S3Event import S3Event
from extraction.AbstractMetadataExtractor import AbstractMetadataExtractor

logger = logging.getLogger(__name__)


class CompositeExtraction(AbstractMetadataExtractor):

    def __init__(self, s3, extractors: list[AbstractMetadataExtractor]):
        super().__init__(s3)
        self.extractors = extractors

    def applies_to(self, s3event: S3Event) -> bool:
        return any(extractor.applies_to(s3event) for extractor in self.extractors)

    def extract(self, s3event: S3Event) -> dict:
        metadata = {}
        for extractor in self.extractors:
            if extractor.applies_to(s3event):
                metadata = metadata | extractor.extract(s3event)

        return metadata
