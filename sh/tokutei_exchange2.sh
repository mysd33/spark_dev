#!/bin/sh

sed -e ':a' -e 'N' -e '$!ba' -e 's/\n//g' kensin_kihon_tokutei.xml > kensin_kihon_tokutei_result2.xml
