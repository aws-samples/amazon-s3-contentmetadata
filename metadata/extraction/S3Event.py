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
from enum import Enum


class EventType(Enum):
    CREATED = "Object Created"
    DELETED = "Object Deleted"


class S3Event:

    def __init__(self, event_type: EventType, event_time: datetime, bucket: str, s3key: str, version_id: str, sequencer: str, etag: str, deletion_type: str):
        self.event_type = event_type
        self.event_time = event_time
        self.bucket = bucket
        self.s3key = s3key
        self.version_id = version_id
        self.sequencer = sequencer
        self.etag = etag
        self.deletion_type = deletion_type

    def __repr__(self):
        return (f"["
                f"'event_type': {self.event_type}, "
                f"'event_time': {self.event_time}, "
                f"'bucket': {self.bucket}, "
                f"'s3key': {self.s3key}, "
                f"'version_id': {self.version_id}, "
                f"'sequencer': {self.sequencer}, "
                f"'etag': {self.etag}, "
                f"'deletion_type': {self.deletion_type}]")

    @staticmethod
    def from_event(event):
        if 'Records' in event:  # S3 'Direct'
            raise NotImplementedError("Handling events 'directly' from S3 is not yet implemented.")

        else:  # Via EventBridge
            return S3Event(
                event_type=EventType(event['detail-type']),
                event_time=datetime.fromisoformat(event['time']),
                bucket=event['detail']['bucket']['name'],
                s3key=event['detail']['object']['key'],
                version_id=event['detail']['object'].get('version-id'),
                etag=event['detail']['object'].get('etag'),
                sequencer=event['detail']['object']['sequencer'],
                deletion_type=event['detail'].get('deletion-type')
            )
