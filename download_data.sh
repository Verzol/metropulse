#!/bin/bash
cd ~/metropulse/data/raw

# Danh sách các loại taxi và năm
TYPES=("yellow" "green")
YEARS=("2023" "2024")

for TYPE in "${TYPES[@]}"
do
    for YEAR in "${YEARS[@]}"
    do
        echo "--- Đang tải dữ liệu $TYPE taxi năm $YEAR ---"
        for MONTH in {01..12}
        do
            URL="https://d37ci6vzurychx.cloudfront.net/trip-data/${TYPE}_tripdata_${YEAR}-${MONTH}.parquet"
            wget -nc "$URL" 
        done
    done
done

echo "Đã tải xong toàn bộ dữ liệu Yellow và Green Taxi (2023-2024)!"
