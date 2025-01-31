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

import base64
import typing

import boto3
import json
import logging
import os

from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
from extraction.CompositeExtraction import CompositeExtraction
from extraction.ExifExtraction import ExifExtraction
from extraction.LabelExtraction import LabelExtraction
from extraction.S3Event import S3Event, EventType

TABLE_NAME = os.environ['METADATA_TABLE']
LOG_LEVEL = os.environ['LOG_LEVEL']

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger()

s3 = boto3.resource("s3")
ddb = boto3.resource("dynamodb")
table = ddb.Table(TABLE_NAME)

# TODO: Make the set of extractors configurable, ideally via ENV
extractor = CompositeExtraction(s3, [
    LabelExtraction(s3, boto3.client("rekognition")),
    ExifExtraction(s3)
])


def make_return(status: int, action: str) -> dict[str, typing.Any]:
    """Utility function for generating a return value from a status and action parameter."""

    return {
        'status': status,
        'body': {
            'action': action
        }
    }


def generate_primary_key(s3event: S3Event) -> dict[str, str]:
    """Generates the 'primary' key for the DDB table from the event.

    At the moment, this is effectively `b64Encode(bucket/key)-<versionID | 0>`
    :param s3event: the event for which to generate the primary key
    :return: the primary key for the DDB table insert/update
    """

    full_key_encoded = base64.b64encode(bytes(f"{s3event.bucket}/{s3event.s3key}", "utf-8")).decode("utf-8")
    # Todo: Test with versioning disabled to check what we send in that case (probably 'null' string instead of nothing)
    return {
        's3key': f"{full_key_encoded}-{'0' if not s3event.version_id else s3event.version_id}",
    }


def is_outdated(s3event: S3Event) -> bool:
    """Determines if the provided event has been superseded by a subsequent event.

    For example, a PUT & Delete may have
    occurred near-simultaneously and the Delete event may have been processed before the PUT event completed.  In this
    case, we want the PUT event to be, effectively, ignored.
    :param s3event:
    :return: True if the provided event has been superseded.
    """

    # Check DDB for superseding event or prior processing
    try:
        response = table.get_item(
            Key=generate_primary_key(s3event)
        )
        if "Item" in response:
            item = response['Item']
            if s3event.sequencer <= item['sequencer']:
                return False

        return True
    except ClientError as err:
        logger.error(
            "Metadata update failed due to: %s: %s",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"]
        )
        raise err


def insert_entry(s3event: S3Event, content: dict) -> bool:
    """Inserts or updates the corresponding row for the event in DDB

    This method may return false if the write failed due to a conditional failure, for instance if another instance
    of this event (a duplicate) or a superseding event has been processed since we checked at the beginning of
    the processing.

    :param s3event:
    :param content:
    :return: True if the insert was successful, false otherwise.
    """

    # Upload Metadata to DDB
    try:
        response = table.put_item(
            Item={
                **generate_primary_key(s3event),
                "bucket": s3event.bucket,
                "key": s3event.s3key,
                "version_id": s3event.version_id,
                "sequencer": s3event.sequencer,
                "latest_event_time": s3event.event_time.isoformat(),
                "expire_at": int((datetime.now(timezone.utc) + timedelta(days=1)).timestamp()),
                **content
            },
            ConditionExpression="NOT attribute_exists(s3key) OR :newSeq > sequencer",
            ExpressionAttributeValues={
                ':newSeq': s3event.sequencer
            }
        )
        logger.info(json.dumps(response, indent=2))
    except ClientError as err:
        # TODO: Differentiate between conditional update failed and something actually errorific
        logger.error(
            "Metadata update failed due to: %s: %s",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"]
        )
        return False

    return True


def handle_create(s3event: S3Event):
    """
    Handles ObjectCreated Events from S3.
    """

    if not is_outdated(s3event):
        return make_return(200, "Outdated")

    # Generate Metadata and insert into DDB
    metadata = extractor.safe_extract(s3event)
    if not metadata:
        metadata = {}

    if insert_entry(s3event, {
        "etag": s3event.etag,
        "sequencer": s3event.sequencer,
        "metadata": json.dumps(metadata)
    }):
        return make_return(200, "Metadata Updated")
    else:
        return make_return(200, "Outdated")


def handle_delete(s3event: S3Event):
    """
    Handles ObjectDeleted events from S3.
    """

    if not is_outdated(s3event):
        return make_return(200, "Outdated")

    if insert_entry(s3event, {
        "deleted": True,
    }):
        return make_return(200, 'Deleted')
    else:
        return make_return(200, 'Outdated')


def lambda_handler(event, context):
    """
    Main entry point for Lambda invocations.
    """

    logger.debug("Making a small change to trigger rebuild")
    logger.debug(json.dumps(event))

    # Grab event details
    s3object = S3Event.from_event(event)

    outcome = {
        "status": 500,
        "body": "Unrecognized Event Type"
    }

    if s3object.event_type == EventType.DELETED:
        logger.debug("Deleted Event")
        outcome = handle_delete(s3object)
    elif s3object.event_type == EventType.CREATED:
        logger.debug("Created Event")
        outcome = handle_create(s3object)

    logger.info(json.dumps(outcome))
    return outcome


if __name__ == "__main__":
    sample_event = {
        'detail-type': 'Object Created',
        'time': datetime.now().isoformat(),
        'detail': {
            'bucket': {
                'name': 's3contentmetadataexamplestack-inputd60cef7a-cuwycl6eodsu'
            },
            'object': {
                'key': '20240820_201808.jpg',
                'etag': '"1a58ea1ae8e96a9415258423fc223992"',
                'sequencer': '125'
            }
        }
    }
    lambda_handler(sample_event, None)