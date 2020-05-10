#!/bin/sh

sed -e '/MN,/!s/^/\x00/g' 11_RECODEINFO_MED.CSV > 11_RECODEINFO_MED_temp.CSV 
sed -e '/MN,/s/^/\x01/g' 11_RECODEINFO_MED_temp.CSV > 11_RECODEINFO_MED_temp2.CSV
sed -e '1 s/\x01//g' 11_RECODEINFO_MED_temp2.CSV > 11_RECODEINFO_MED_temp3.CSV
sed -e ':a' -e 'N' -e '$!ba' -e 's/\n//g' 11_RECODEINFO_MED_temp3.CSV > 11_RECODEINFO_MED_temp4.CSV
sed -e 's/\x01/\r\n/g' 11_RECODEINFO_MED_temp4.CSV > 11_RECODEINFO_MED_result2.CSV
rm 11_RECODEINFO_MED_temp*.CSV