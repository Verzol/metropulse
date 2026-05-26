# EDA Report

## Phạm Vi

Ngày kiểm chứng: `2026-05-26` (UTC).

## Trạng Thái Triển Khai

| Notebook | Trạng thái | Proof được bổ sung |
|---|---|---|
| `00_spark_minio_setup.ipynb` | Đã có | Path helpers dùng được environment overrides giống pipeline |
| `01_bronze_eda.ipynb` | Đã sửa | Event volume, Kafka offset duplicate, metadata và payload-size aggregate |
| `02_silver_transform_eda.ipynb` | Đã sửa | Critical null invariant, weather coverage, weather-hour cardinality và dedup output invariant |
| `03_silver_quality_clean_eda.ipynb` | Đã sửa | Lineage mismatch detector, retention/candidate counts và cleaning-rule violation checks |

## Kết Quả Dữ Liệu

### Bronze

| Dataset | Rows | Duplicate Kafka offset groups | Avg payload chars | P95 payload chars | Max payload chars |
|---|---:|---:|---:|---:|---:|
| `yellow_taxi` | 107,580,599 | 0 | 582.26 | 591 | 601 |
| `green_taxi` | 1,447,278 | 0 | 594.75 | 607 | 618 |
| `weather` | 17,544 | 0 | 389.41 | 395 | 398 |

Kết luận: Bronze đang giữ được raw Kafka events ở grain nhỏ, và bộ khóa `topic`, `partition`, `offset` không bị trùng trong snapshot được đọc. Payload size phù hợp với transport JSON event ở quy mô prototype; nó không phải là chứng minh throughput production.

### Silver Enriched Và Weather

| Metric | Giá trị |
|---|---:|
| `silver/hourly_weather` unique hours | 17,542 |
| Duplicate weather hours | 0 |
| Maximum rows per weather hour | 1 |
| `silver/taxi_weather_trips` rows | 60,521,651 |
| Null `weather_timestamp` rows | 0 |
| Null `temperature_f` rows | 0 |
| Null `precipitation_mm` rows | 0 |
| Weather timestamp coverage ratio | 100.00% |

Kết luận: weather side có cardinality một row mỗi giờ, nên phép left join theo `pickup_hour = weather_hour` không thể tự nó nhân thêm taxi rows. Weather coverage của Silver enriched hiện tại là đầy đủ.

### Silver Core Và Clean

| Metric | Giá trị |
|---|---:|
| `silver/taxi_weather_trips_core` rows | 80,922,997 |
| `silver/taxi_weather_trips_clean` rows | 80,922,997 |
| Gold candidate rows | 78,272,751 |
| Non-candidate rows | 2,650,246 |
| Null candidate status rows | 0 |
| Gold candidate ratio | 96.72498% |
| Quality report checks marked `pass` | 68 |

Cleaning-rule invariants trên Clean snapshot:

| Source column | Source null rows | Imputation violations | Preservation violations |
|---|---:|---:|---:|
| `passenger_count` | 5,478,285 | 0 | 0 |
| `ratecode_id` | 5,478,285 | 0 | 0 |
| `payment_type` | 79,941 | 0 | 0 |
| `congestion_surcharge` | 5,478,285 | 0 | 0 |
| `airport_fee` | 77,928,015 | 0 | 0 |

Kết luận: rules imputation được áp dụng nhất quán trên Clean snapshot đang lưu. `airport_fee` thiếu rất nhiều là dự kiến khi schema green taxi không có field này; missing flag cần được giữ làm tín hiệu thay vì diễn giải như zero fee từ nguồn.

### Gold Source Readiness: Hourly Weather Và Taxi Weather Core

Thiết kế Gold hiện tại chọn hai nguồn trực tiếp:

```text
s3a://silver/hourly_weather/
s3a://silver/taxi_weather_trips_core/
```

| Kiểm chứng | Kết quả |
|---|---:|
| `hourly_weather` rows | 17,542 |
| Distinct `weather_hour` | 17,542 |
| Duplicate `weather_hour` groups | 0 |
| Null weather feature rows trong `hourly_weather` | 0 |
| `taxi_weather_trips_core` rows | 80,922,997 |
| Duplicate Core trip-key groups | 0 |
| Null critical Core fact/weather rows | 0 |
| Core rows không match `weather_hour` hiện tại | 0 |
| Core rows lệch weather features sau khi cast weather về Core schema | 0 |
| Core outlier rows | 2,650,246 |
| Core non-outlier rows | 78,272,751 |

Weather dùng kiểu `double`, trong khi Core lưu các numeric weather features theo kiểu compact như `float`/`smallint`. Vì vậy phép đối chiếu tính nhất quán được thực hiện sau khi cast weather về schema Core; sau chuẩn hóa kiểu dữ liệu, toàn bộ Core rows khớp hourly weather hiện tại.

Kết luận: hai bảng đạt điều kiện dữ liệu để làm nguồn cho Gold. `hourly_weather` có grain một row mỗi giờ và Core có fact schema gọn, không null critical, không duplicate business key, không lệch weather dimension. Core đã chứa weather features khớp với dimension, nên không cần join lại weather vào từng trip nếu Gold chỉ sử dụng các feature đó; `hourly_weather` phù hợp khi tạo canonical hourly timeline hoặc aggregate weather riêng. Tuy nhiên Core chưa phải dataset đã loại outlier hoặc impute nullable operational fields; Gold transform phải ghi rõ rule lọc và null handling theo mục tiêu BI/ML.

## Phát Hiện Cần Xử Lý

`silver/taxi_weather_trips` hiện có `60,521,651` rows, trong khi Core và Clean đều có `80,922,997` rows. Chênh lệch `20,401,346` rows cho thấy Silver enriched và downstream Core/Clean/quality report không thuộc cùng một lần materialization hiện tại.

Do đó:

- `68` quality checks pass chỉ áp dụng cho Core snapshot đang lưu, không chứng minh Core khớp với Silver enriched hiện tại.
- `78,272,751` Gold candidates là proof của Clean snapshot đang lưu, chưa được phép diễn giải là kết quả của Silver enriched `60,521,651` rows.
- Notebook `03` đã được sửa để báo `fail_outputs_not_same_snapshot` khi mismatch này xảy ra.
- Với thiết kế Gold mới, compatibility giữa `hourly_weather` và Core đã được chứng minh trực tiếp. Tuy vậy, để lineage end-to-end không gây tranh luận khi review, nên rebuild `silver-core` và chạy lại quality checks sau lần materialize enriched gần nhất trước khi ghi Gold output chính thức.

## Phạm Vi Gold

Tại thời điểm kiểm chứng, các path sau chưa tồn tại:

| Expected Gold dataset | Trạng thái |
|---|---|
| `hourly_demand_features` | Missing |
| `zone_weather_correlation` | Missing |
| `pickup_forecast_dataset` | Missing |
| `borough_traffic_aggregation` | Missing |

Gold layer chưa được triển khai trong snapshot hiện tại, nên không có notebook Gold trong phạm vi EDA nộp lần này. Nguồn đầu vào đã được chọn và kiểm chứng cho bước triển khai kế tiếp là `hourly_weather` và `taxi_weather_trips_core`; số liệu Clean/`is_gold_candidate` được giữ như bằng chứng phụ về tác động của policy loại outlier, không phải source contract của Gold.
