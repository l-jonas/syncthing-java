#!/bin/bash

set -e

NEW_VERSION_NAME=$1
OLD_VERSION_NAME=$(grep " version =" "build.gradle" | awk '{print $3}' | tr -d "\"")
if [[ -z ${NEW_VERSION_NAME} ]]
then
    echo "New version name is empty. Please set a new version. Current version: $OLD_VERSION_NAME"
    exit
fi

echo "

Updating Version
-----------------------------
"
sed -i "s/version = \"$OLD_VERSION_NAME\"/version = \"$NEW_VERSION_NAME\"/" "build.gradle"

git add "build.gradle"
git commit -m "Version $NEW_VERSION_NAME"
git tag ${NEW_VERSION_NAME}

echo "
Update ready.
"
