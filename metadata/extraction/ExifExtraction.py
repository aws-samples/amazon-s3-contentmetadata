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
import decimal
import logging

from extraction.S3Event import S3Event
from extraction.AbstractMetadataExtractor import AbstractMetadataExtractor, key_extension, walk_dict
from PIL import Image, ExifTags, TiffImagePlugin

logger = logging.getLogger(__name__)


def conversion(item):
    if isinstance(item, TiffImagePlugin.IFDRational):
        return {
            'type': 'IFDRational',
            'denominator': item.denominator,
            'imag': item.imag,
            'numerator': item.numerator
        }
    elif isinstance(item, decimal.Decimal):
        return str(item)
    elif isinstance(item, bytes):
        return base64.b64encode(item).decode("utf-8")
    else:
        return item


class ExifExtraction(AbstractMetadataExtractor):

    def applies_to(self, s3event: S3Event) -> bool:
        return key_extension(s3event).lower() in ('.jpg', '.jpeg', '.png')

    def extract(self, s3event: S3Event) -> dict:
        with Image.open(self.fetch_object(s3event)) as img:
            exif_data = {
                ExifTags.TAGS[k]: v
                for k, v in img._getexif().items()
                if k in ExifTags.TAGS and ExifTags.TAGS[k] != 'MakerNote' and ExifTags.TAGS[k] != 'UserComment'
            } if img._getexif() else {}

            if 'GPSInfo' in exif_data:
                gps_info = {}
                for key in exif_data['GPSInfo'].keys():
                    decode = ExifTags.GPSTAGS.get(key, key)
                    value = exif_data['GPSInfo'][key]
                    gps_info[decode] = value
                exif_data['GPSInfo'] = gps_info

            walk_dict(exif_data, conversion)

            width, height = img.size
            return {
                "exif": exif_data,
                "image": {
                    "image_height": height,
                    "image_width": width,
                    "format": img.format
                }
            }
