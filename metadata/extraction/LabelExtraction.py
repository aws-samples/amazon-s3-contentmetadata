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

import boto3
import logging

from extraction.S3Event import S3Event
from extraction.AbstractMetadataExtractor import AbstractMetadataExtractor, key_extension

logger = logging.getLogger(__name__)


class LabelExtraction(AbstractMetadataExtractor):

    def __init__(self, s3, rk):
        super().__init__(s3)
        self.rk = rk

    def applies_to(self, s3event: S3Event) -> bool:
        return key_extension(s3event).lower() in ('.jpg', '.jpeg', '.png')

    def extract(self, s3event: S3Event) -> dict:
        try:
            response = self.rk.detect_labels(
                Image={
                    'S3Object': {
                        'Bucket': s3event.bucket,
                        'Name': s3event.s3key,
                        **({} if not s3event.version_id else {'Version': s3event.version_id})
                    }
                },
                MaxLabels=3
            )
            labels = [label for label in response["Labels"]]
            return {
                "labels": labels
            }

        except self.rk.exceptions.InvalidS3ObjectException:
            return {}
