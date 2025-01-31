#!/bin/bash
#
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

# Images were sourced from https://www.pixabay.com

#Store input parameter in variable
if [ $# -eq 0 ]; then
    echo "Error: No bucket name provided."
    echo "Usage: $0 <bucket_name>"
    exit 1
fi


#Assign the first argument to the bucket_name variable
bucket_name=$1

# Print the bucket name
echo "Bucket name: $bucket_name"

# Function to upload objects with tags
function upload_file_with_tag() {
    local bucketname=$1
    local filename=$2
    local tagname=$3
    # if tagname is empty run a put-object with --tagging option
    if [ -z "$tagname" ]; then
            echo "Uploading $filename without tag"
            aws s3api put-object --body $filename --bucket $bucketname --key $filename --no-cli-pager
    else
            echo "Uploading $filename with tag $tagname"
            aws s3api put-object --body $filename --bucket $bucketname --key $filename --tagging "Project=$tagname" --no-cli-pager
    fi
}

# Upload objects with object tag Project=Africa
files=("elephant-boy-jungle.jpg" "girl-with-elephant.jpg" "safari-elephant.jpg" "tiger-on-tree.jpg" "ball-in-stadium.jpg" "man-soccerball.jpg" "landscape-bridge-river.jpg" "river-trees-mountain.jpg")
for file in "${files[@]}"; 
do
    upload_file_with_tag $bucket_name $file "Africa"
done

# Upload objects with object tag Project=Europe
files=("insideroom-table.jpg" "coffeemug-table.jpg" "furniture-table.jpg" "elephant-europe.jpg" "home-office-coffeemug.png" "landscapes-beach-person.jpg" "grapevine.jpg")
for file in "${files[@]}"; 
do
    upload_file_with_tag $bucket_name $file "Europe"
done

# Upload objects with no tags
files=("mountain-biking-man-jump.jpg" "person-walk-park.jpg" "jetty-women-boardwalk.jpg")
for file in "${files[@]}"; 
do
    upload_file_with_tag $bucket_name $file 
done


# For example, let's create a simple function to simulate changes to object state
function simulate_changes_objects() {
    local bucketname=$1
    # Files for Project A - Africa with elephant
    aws s3api put-object --body soccer.jpg --bucket $bucketname --key soccer.jpg --tagging "Project=WrongProject" --no-cli-pager
    # Delete object & reupload
    aws s3api delete-object --bucket $bucketname --key soccer.jpg --no-cli-pager
    aws s3api put-object --body soccer.jpg --bucket $bucketname --key soccer.jpg --tagging "Project=Africa" --no-cli-pager
    # update Tag
    aws s3api put-object-tagging --bucket $bucketname --key soccer.jpg --tagging '{ "TagSet": [{"Key": "ObjectInfo", "Value": "Soccer Ball"}, {"Key": "Project", "Value": "Africa"}]}' --no-cli-pager
}

# Call the function with the provided bucket name
simulate_changes_objects "$bucket_name"

# Completion message
echo "Script completed successfully"