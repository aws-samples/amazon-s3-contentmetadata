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
import collections
import logging
import os
import typing

from abc import ABC, abstractmethod
from botocore.exceptions import ClientError
from extraction.S3Event import S3Event

logger = logging.getLogger(__name__)


class ObjectDoesNotExist(Exception):
    """Thrown when the object doesn't exist, or the ETag does not match."""

    pass


def key_extension(s3event: S3Event) -> str:
    """Utility function for determining the file extension from an S3 Key."""

    _, ext = os.path.splitext(s3event.s3key)
    return ext


def walk_dict(collection: dict, f: collections.abc.Callable[[typing.Any], None]) -> None:
    for key, item in collection.items():
        if isinstance(item, dict):
            walk_dict(item, f)
        elif isinstance(item, list):
            walk_list(item, f)
        elif isinstance(item, tuple):
            collection[key] = tuple(map(f, collection))
        else:
            collection[key] = f(item)


def walk_list(collection: list, f: collections.abc.Callable[[typing.Any], None]) -> None:
    for i, item in enumerate(collection):
        if isinstance(item, dict):
            walk_dict(item, f)
        elif isinstance(item, list):
            walk_list(item, f)
        elif isinstance(item, tuple):
            collection[i] = tuple(map(f, item))
        else:
            collection[i] = f(item)



class AbstractMetadataExtractor(ABC):
    """Base class for defining a metadata extractor"""

    def __init__(self, s3):
        self.s3 = s3

    @abstractmethod
    def applies_to(self, s3event: S3Event) -> bool:
        """Allows the extraction implementation to decide if metadata extraction applies to this event/object.

        If it does apply, then extract will be called to perform the metadata generation, otherwise processing
        will stop.

        :param s3event: the event representing the object change
        :return: true iff the extractor applies to the event, e.g. can perform the extraction of EXIF from a JPEG
        """
        pass


    def fetch_object(self, s3event: S3Event):
        """Utility function for fetching the version/instance of an object referenced in an event."""

        bucket = self.s3.Bucket(s3event.bucket)
        try:
            return bucket.Object(s3event.s3key).get(
                IfMatch=s3event.etag,
                **{'VersionId': s3event.version_id} if s3event.version_id else {}
            )["Body"]
        except (self.s3.meta.client.exceptions.NoSuchKey,
                self.s3.meta.client.exceptions.NoSuchBucket) as err:
            logger.info(
                "Metadata update failed due to: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"]
            )
            raise ObjectDoesNotExist()
        except ClientError as err:
            if err.response['Error']['Code'] == 'PreconditionFailed':
                raise ObjectDoesNotExist()

            logger.info(
                "Metadata update failed due to: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"]
            )
            raise err


    @abstractmethod
    def extract(self, s3event: S3Event) -> dict:
        """Implementation specific metadata generation/extraction.

        :param s3event: the event representing the object change
        :return: a dict containing the metadata for this object
        """

        pass

    def safe_extract(self, s3event: S3Event):
        try:
            return self.extract(s3event)
        except ObjectDoesNotExist:
            return {}
