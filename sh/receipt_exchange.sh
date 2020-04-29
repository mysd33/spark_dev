#!/bin/sh

sed -e '/MN,/s/^/\x00/g' 11_RECODEINFO_MED.CSV > 11_RECODEINFO_MED_temp.CSV 
sed -e '1 s/\x00//g' 11_RECODEINFO_MED_temp.CSV > 11_RECODEINFO_MED_temp2.CSV
sed -e 's/$/\r/g' 11_RECODEINFO_MED_temp2.CSV > 11_RECODEINFO_MED_result.CSV 
rm 11_RECODEINFO_MED_temp*.CSV
