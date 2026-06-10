# MetroPulse Final Report: Content and Figure Guide

## 1. Mục đích tài liệu

Tài liệu này rà soát bố cục hiện tại trong `reports/final_report/` và đề xuất
cách hoàn thiện báo cáo cuối kỳ dựa trên những thành phần thực sự có trong
repository.

Phạm vi cần được giữ nhất quán trong toàn bộ báo cáo:

- Data pipeline: Kafka -> Spark -> MinIO Bronze/Silver/Gold.
- Orchestration: Apache Airflow.
- Serving: Gold/MinIO -> PostgreSQL -> FastAPI -> Streamlit.
- Machine Learning: XGBoost cho 3 bài toán "Demand Prediction", "Fare Estimation" và "Tip Estimation".
- Không trình bày Power BI, Transformer, LightGBM hoặc bất kỳ công nghệ nào khác chưa được triển khai thực tế trong repository.

Repository có `gold_demand_features` và dashboard demand mart, nhưng việc có
dataset không đồng nghĩa nhóm đã chọn demand forecasting làm bài toán ML cuối
cùng. Có thể mô tả chúng như sản phẩm dữ liệu/phân tích của Gold, không đưa vào
phần thực nghiệm ML chính.

## 2. Đánh giá bố cục hiện tại

Báo cáo hiện có sáu phần:

1. Tổng quan dự án.
2. Dữ liệu và bài toán nghiệp vụ.
3. Kiến trúc Big Data và thiết kế hệ thống.
4. Data Pipeline, Gold Layer và Feature Engineering.
5. Machine Learning và kết quả thực nghiệm.
6. Đánh giá hệ thống, hạn chế và hướng phát triển.

Cấu trúc sáu phần này hợp lý và không cần tách thêm chương. Vấn đề chính nằm ở
nội dung bên trong:

- Phần 1 và Phần 2 đang đặt demand forecasting làm mục tiêu trung tâm, chưa
  phản ánh lựa chọn ML cuối cùng là Fare/Tip.
- Phần 1 và Phần 3 còn nhắc Power BI; cần xóa và thay bằng Streamlit.
- Phần 3 có nội dung Serving tương đối đầy đủ nhưng đang trộn giữa thiết kế
  kiến trúc, hướng dẫn vận hành và mô tả giao diện. Nên rút gọn phần lệnh chạy,
  chuyển trọng tâm sang luồng dữ liệu và contract giữa các tầng.
- Phần 4 mô tả pipeline chi tiết và có nhiều số liệu hữu ích. Tuy nhiên, phần
  feature engineering cần ưu tiên `gold_fare_tip_features`; demand features chỉ
  nên được mô tả như một Gold analytical dataset.
- Phần 5 hiện gần như phải viết lại toàn bộ vì đang nói về demand forecasting,
  các mô hình đề xuất và kết quả để trống. Nội dung đúng phải là hai baseline
  XGBoost Fare và Tip cùng kết quả đã lưu trong `ml/logs/`.
- Phần 6 đang đánh giá demand forecasting là bài toán ML chính. Cần thay bằng
  đánh giá riêng cho Fare và Tip, đặc biệt phải nêu rõ Tip model còn yếu.
- Thư mục `figures/` hiện chỉ có logo UET. Các khung `\fbox` trong báo cáo mới
  là placeholder, chưa phải hình báo cáo.
- `references.bib` đang trống. Nếu báo cáo giữ các nhận định hoặc dẫn nguồn bên
  ngoài thì phải bổ sung tài liệu tham khảo; nếu không, cần viết lại thành mô tả
  kết quả nội bộ, tránh trích dẫn ngầm.

## 3. Bố cục nội dung đề xuất

### Phần 1. Tổng quan dự án

Mục tiêu của phần này là trả lời ngắn gọn: MetroPulse giải quyết vấn đề gì,
phạm vi nào đã hoàn thành và đóng góp kỹ thuật chính là gì.

#### 1.1. Bối cảnh và động lực

Nên trình bày:

- Dữ liệu taxi NYC có quy mô lớn, chứa thông tin thời gian, vị trí, quãng
  đường, fare, tip và payment type.
- Dữ liệu thời tiết lịch sử theo giờ được dùng để làm giàu dữ liệu chuyến đi.
- Dự án tập trung xây dựng pipeline dữ liệu lớn có phân lớp, kiểm tra chất
  lượng, serving và baseline ML.

Không nên khẳng định:

- Hệ thống dự báo nhu cầu là mục tiêu ML chính.
- Hệ thống đã dùng Power BI.
- Hệ thống đạt chuẩn production hoặc có khả năng mở rộng đã được kiểm chứng.

#### 1.2. Mục tiêu dự án

Nên chia thành bốn mục tiêu:

1. Xây dựng ingestion và batch-fed streaming bằng Kafka/Spark.
2. Xây dựng Medallion Lakehouse trên MinIO.
3. Xuất bản dữ liệu Gold sang PostgreSQL và cung cấp dashboard qua
   FastAPI/Streamlit.
4. Xây dựng baseline XGBoost cho Fare Estimation và Tip Estimation.

#### 1.3. Phạm vi

Giữ bảng phạm vi nhưng sửa:

- Consumer thành `FastAPI/Streamlit dashboard và XGBoost baseline`.
- Xóa toàn bộ Power BI.
- Nêu rõ single-host GCP VM, Kafka single broker, Spark hai workers và
  replication factor 1.
- Không đưa các công nghệ chưa triển khai vào cột trong phạm vi.

#### 1.4. Đóng góp chính

Nên ghi nhận:

- Pipeline end-to-end từ producer đến Serving.
- Bronze lưu payload và Kafka metadata.
- Silver chuẩn hóa schema, timezone, deduplication và weather enrichment.
- Gold tạo feature datasets và dashboard marts.
- PostgreSQL publication có staging, promotion, validation và read-only roles.
- FastAPI/Streamlit cung cấp giao diện sử dụng dữ liệu.
- Hai baseline XGBoost cho Fare và Tip.

### Phần 2. Dữ liệu và bài toán nghiệp vụ

Phần này nên chuyển trọng tâm từ demand forecasting sang hai bài toán ML cuối
cùng, đồng thời vẫn giữ dashboard demand như một use case phân tích.

#### 2.1. Bài toán nghiệp vụ

Đề xuất mô tả ba use case:

- Phân tích nhu cầu taxi theo thời gian và pickup zone trên dashboard.
- Dự đoán `fare_amount` từ đặc trưng chuyến đi, thời gian và thời tiết.
- Dự đoán `tip_percent` cho các chuyến thanh toán bằng thẻ.

#### 2.2. Câu hỏi phân tích

Nên dùng các câu hỏi:

- Nhu cầu taxi thay đổi như thế nào theo thời gian và pickup zone?
- Fare liên hệ như thế nào với trip distance, pickup/dropoff zone, thời gian
  và weather?
- Tip percent thay đổi như thế nào theo payment type, thời gian và đặc trưng
  chuyến đi?
- Vì sao Tip model chỉ sử dụng `payment_type = 1`?
- Dữ liệu cần được tổng hợp ra sao để Streamlit không quét bảng trip-level lớn?

#### 2.3. Nguồn dữ liệu

Giữ ba nguồn đã có:

- NYC Yellow/Green Taxi Trip Data.
- Open-Meteo Historical Weather.
- Taxi Zone Lookup.

Mỗi nguồn cần có: grain, trường chính, khoảng dữ liệu thực tế được pipeline sử
dụng và vai trò trong Silver/Gold.

#### 2.4. EDA và chất lượng dữ liệu ban đầu

Nên trình bày bằng bảng và biểu đồ thay vì chỉ liệt kê:

- Khác biệt schema Yellow/Green.
- Null theo các cột quan trọng.
- Phân phối `trip_distance`, `fare_amount`, `tip_percent`.
- Tỷ lệ payment type.
- Bản ghi âm, bằng 0 hoặc outlier.
- Cardinality của weather theo giờ.

Chỉ sử dụng số liệu lấy trực tiếp từ notebook/artifact đã chạy. Không điền số
liệu ước lượng bằng mắt.

### Phần 3. Kiến trúc Big Data và thiết kế hệ thống

Phần này chỉ nên giải thích kiến trúc và trade-off, không lặp lại chi tiết biến
đổi của từng lớp.

#### 3.1. Sơ đồ kiến trúc tổng thể

Luồng chính:

```text
Taxi files / Open-Meteo
        -> Producers
        -> Kafka
        -> Spark Structured Streaming
        -> Bronze/MinIO
        -> Spark batch jobs
        -> Silver/MinIO
        -> Gold/MinIO
        -> PostgreSQL
        -> FastAPI
        -> Streamlit

Gold/PostgreSQL
        -> XGBoost Fare/Tip training
```

Airflow đặt phía trên, nối tới các external jobs mà nó điều phối. Không vẽ
Airflow nằm trong data path.

#### 3.2. Tech stack

Sửa presentation thành Streamlit. Xóa Power BI. ML ghi XGBoost, scikit-learn,
pandas và NumPy, nhưng cần nói rõ pandas chỉ được dùng trên dữ liệu sampling
trong bước ML, không dùng cho ETL dữ liệu lớn.

#### 3.3. Deployment topology

Mô tả:

- Một GCP VM `e2-standard-16`.
- Docker Compose.
- Kafka + Zookeeper.
- Spark master + hai workers.
- MinIO.
- Airflow webserver/scheduler/metadata PostgreSQL.
- PostgreSQL Warehouse và pgAdmin.
- FastAPI/Streamlit chạy trên VM và truy cập qua localhost/SSH tunnel.

Phần này cần giải thích tác động của single-host: không có host-level fault
tolerance và các dịch vụ cạnh tranh CPU/RAM/I/O.

#### 3.4. Serving architecture

Giữ luồng:

```text
Gold Parquet -> Spark JDBC -> PostgreSQL staging
             -> transactional promotion to ml/mart
             -> validation/audit
             -> FastAPI read-only endpoints
             -> Streamlit
```

Liệt kê đúng endpoint hiện có:

- `/api/health`
- `/api/meta`
- `/api/summary`
- `/api/hourly-demand`
- `/api/zone-summary`
- `/api/payment-tip-summary`

Không cần đưa hướng dẫn mở hai terminal vào phần kiến trúc; nội dung vận hành
đó đã phù hợp hơn với `docs/POSTGRES_WAREHOUSE_DASHBOARD_HANDOFF.md`.

### Phần 4. Data Pipeline, Gold Layer và Feature Engineering

Đây nên là phần kỹ thuật chi tiết nhất của báo cáo.

#### 4.1. Bronze

Trình bày:

- Input topic.
- Explicit output schema của wrapper Bronze.
- Raw `json_data` và Kafka metadata.
- Parquet partition theo `ingestion_date`.
- Checkpoint và `availableNow`.
- Lý do không clean/deduplicate/enrich tại Bronze.

#### 4.2. Silver

Trình bày theo flow:

1. Parse JSON bằng explicit schema.
2. Chuẩn hóa Yellow/Green bằng `unionByName`.
3. Chuẩn hóa timezone `America/New_York`.
4. Deduplicate Kafka key và business key.
5. Chuẩn hóa weather một dòng mỗi giờ.
6. Broadcast join weather vào taxi theo `pickup_hour`.
7. Tạo Core/Clean, quality flags và missing flags.

Phải giải thích chi phí shuffle của deduplication và lợi ích của broadcast join.
Không nên chỉ mô tả tên hàm.

#### 4.3. Gold

Phân biệt rõ:

- `gold_fare_tip_features`: feature table ML chính của báo cáo.
- `gold_demand_features`: analytical/ML-ready dataset đã tạo, nhưng không phải
  thực nghiệm ML chính.
- Ba `dashboard_*` marts: nguồn dữ liệu cho FastAPI/Streamlit.

#### 4.4. Feature engineering cho Fare/Tip

Các feature thực tế từ cấu hình:

- Base: `trip_distance`, `pu_location_id`, `do_location_id`,
  `passenger_count`, `ratecode_id`, `hour`, `day_of_week`, `month`,
  `temperature_f`, `precipitation_mm`.
- Derived: `is_rush_hour`, `is_weekend`, `is_raining`, `is_cold`.
- Fare target: `fare_amount`.
- Tip target: `tip_percent`.
- Tip filter: `payment_type = 1`.

Nêu rõ bước ML còn fill null `passenger_count` và `ratecode_id`, đồng thời lọc
`fare_amount` và `trip_distance` theo rule trong
`ml/train/fare_tip_feature_engineering.py`.

#### 4.5. Data quality và publication

Nên dùng một quality matrix:

| Layer | Check | Failure impact |
|---|---|---|
| Bronze | Kafka offset uniqueness, payload availability | Replay/double ingestion |
| Silver | Critical null, duplicate key, weather coverage | Sai fact hoặc join |
| Gold | Grain uniqueness, range, aggregate total | Sai feature/mart |
| PostgreSQL | Source-target count/aggregate/audit | Serving không khớp Gold |

Nếu giữ cảnh báo mismatch snapshot Silver, phải mô tả đây là hạn chế lineage
đã quan sát, không đồng thời tuyên bố toàn pipeline cùng một snapshot tuyệt đối.

### Phần 5. Machine Learning và kết quả thực nghiệm

Phần này cần viết lại toàn bộ theo cấu trúc sau.

#### 5.1. Định nghĩa hai bài toán

Fare Estimation:

```text
X -> trip/time/location/weather features
y -> fare_amount
```

Tip Estimation:

```text
X -> trip/time/location/weather features
y -> tip_percent
scope -> payment_type = 1
```

#### 5.2. Chuẩn bị dữ liệu

Nêu đúng implementation:

- Nguồn là `ml.gold_fare_tip_features` trong PostgreSQL.
- Script đang lấy sample khoảng 20% theo cấu hình.
- Dữ liệu được clean và tạo bốn derived features.
- Random train/test split 80/20.
- Từ train tiếp tục lấy 10% làm validation cho early stopping.

Không mô tả temporal split nếu code thực tế vẫn dùng random split.

#### 5.3. Thuật toán và cấu hình

Cả hai mô hình dùng `XGBRegressor`:

- Fare: objective `reg:squarederror`.
- Tip: objective `reg:absoluteerror`.
- Tree method: `hist`.
- Fare và Tip có hyperparameter riêng trong `ml/configs/xgb_fare_tip.yaml`.

Nên đưa bảng hyperparameter rút gọn, chỉ gồm các tham số quan trọng:
`n_estimators`, `learning_rate`, `max_depth`, `min_child_weight`,
`subsample`, `colsample_bytree`, objective và early stopping.

#### 5.4. Metric và kết quả

Sử dụng đúng artifact hiện có:

| Model | RMSE | MAE | MAPE | R2 |
|---|---:|---:|---:|---:|
| Fare XGBoost | 3.8714 USD | 1.9973 USD | 0.1258 | 0.9516 |
| Tip XGBoost | 9.9589 điểm % | 6.2401 điểm % | 1.1532 | 0.1476 |

Diễn giải bắt buộc:

- Fare baseline có mức phù hợp cao hơn rõ rệt theo R2 trong lần chạy đã lưu.
- Tip baseline có R2 thấp, cho thấy bộ feature hiện tại giải thích Tip kém hơn.
- Không gọi Tip model là mô hình tốt hoặc chính xác.
- Không so sánh trực tiếp RMSE Fare và Tip vì target khác đơn vị.
- MAPE của Tip cần diễn giải thận trọng vì target có thể bằng hoặc gần 0.

#### 5.5. Phân tích lỗi và feature importance

Nên bổ sung thực nghiệm có thể tái tạo từ model artifact:

- Actual vs predicted cho Fare.
- Actual vs predicted cho Tip.
- Residual distribution cho từng model.
- Feature importance của từng model.
- Error theo khoảng `trip_distance` cho Fare.
- Error theo khoảng `tip_percent` hoặc theo tháng cho Tip.

Nếu chưa tạo các biểu đồ này thì báo cáo chỉ nên trình bày metrics đã lưu và
ghi rõ phân tích chi tiết là phần cần bổ sung; không viết kết luận dựa trên
feature importance tưởng tượng.

### Phần 6. Đánh giá, hạn chế và kết luận

#### 6.1. Đánh giá Data Engineering

Đánh giá theo các bằng chứng:

- Các layer và output thực tế.
- Quality artifacts.
- PostgreSQL publication/validation.
- API endpoint và Streamlit dashboard.
- Trade-off single-host.

Tránh dùng từ "hoàn chỉnh ở mức production"; nên dùng "prototype end-to-end có
định hướng production".

#### 6.2. Đánh giá ML

Tách hai đoạn:

- Fare: baseline cho kết quả tốt hơn, R2 = 0.9516 trong artifact hiện có.
- Tip: kết quả hạn chế, R2 = 0.1476; cần thêm feature hoặc thay đổi cách đặt bài
  toán trước khi ứng dụng.

#### 6.3. Hạn chế

Nên nêu:

- Single-host và Kafka replication factor 1.
- Sampling 20% cho ML.
- Random split có thể không phản ánh temporal/generalization shift.
- Tip target khó mô hình hóa từ feature hiện có.
- Chưa có experiment tracking hoặc model serving endpoint.
- Silver snapshot từng có mismatch lineage.
- Dashboard là demo nội bộ qua SSH tunnel, không phải public production app.

#### 6.4. Hướng phát triển

Chỉ ghi như hướng tương lai:

- Bổ sung feature cho Tip.
- So sánh baseline khác nếu nhóm thực sự chạy thêm.
- Temporal/out-of-time validation.
- Experiment tracking và model versioning.
- Incremental Gold/PostgreSQL refresh.
- Multi-node deployment và managed secret khi vượt phạm vi capstone.

Không đưa Transformer vào hướng phát triển nếu nhóm không có kế hoạch cụ thể
hoặc không có cơ sở lựa chọn.

## 4. Danh mục hình cần chuẩn bị

Nên lưu toàn bộ hình mới trong:

```text
reports/final_report/figures/
```

Tên file dùng lowercase, snake_case và ưu tiên PNG độ phân giải cao hoặc PDF/SVG
cho sơ đồ vector.

### 4.1. Hình bắt buộc

#### F01. Sơ đồ kiến trúc end-to-end

- Tên file: `architecture_overview.pdf`
- Vị trí: Phần 3, sau mục tổng quan kiến trúc.
- Nội dung: Producers, Kafka, Spark, MinIO Bronze/Silver/Gold, Airflow,
  PostgreSQL, FastAPI, Streamlit và nhánh XGBoost Fare/Tip.
- Cần phân biệt data flow và orchestration flow bằng màu hoặc kiểu mũi tên.
- Không có Power BI hoặc Transformer.

#### F02. Deployment topology trên GCP VM

- Tên file: `single_host_deployment.pdf`
- Vị trí: Phần 3, mục triển khai.
- Nội dung: một VM chứa các Docker services; thể hiện các cổng nội bộ, Kafka
  `kafka:29092`, host `localhost:9092`, PostgreSQL localhost binding và SSH
  tunnel cho người dùng.
- Mục tiêu: chứng minh topology single-host và ranh giới kết nối.

#### F03. Medallion data flow

- Tên file: `medallion_data_flow.pdf`
- Vị trí: đầu Phần 4.
- Nội dung: input/output và transformation chính của Bronze, Silver, Gold.
- Nên ghi grain của các output quan trọng thay vì chỉ vẽ ba huy chương.

#### F04. Silver transformation flow

- Tên file: `silver_transformation_flow.pdf`
- Vị trí: Phần 4, mục Silver.
- Nội dung: explicit schema -> normalize Yellow/Green -> timezone ->
  deduplicate -> weather hourly -> broadcast join -> Core/Clean.
- Đánh dấu bước gây shuffle và bước broadcast.

#### F05. PostgreSQL publication và Serving

- Tên file: `serving_publication_flow.pdf`
- Vị trí: cuối Phần 3 hoặc mục publication ở Phần 4, không cần lặp ở cả hai.
- Nội dung: Gold Parquet -> Spark JDBC -> staging -> transaction promotion ->
  `ml`/`mart` -> audit/validation -> FastAPI -> Streamlit.

#### F06. Ảnh dashboard Streamlit tổng quan

- Tên file: `streamlit_dashboard_overview.png`
- Vị trí: Phần 3, mục dashboard demo.
- Chụp giao diện đang chạy thật, có sidebar, KPI cards và ít nhất một biểu đồ.
- Không dùng mockup nếu ứng dụng thật có thể chạy.

#### F07. Dashboard tab Zone hoặc Payment & Tip

- Tên file: `streamlit_payment_tip_tab.png`
- Vị trí: ngay sau F06 hoặc Phần 2 khi mô tả use case.
- Ưu tiên tab `Thanh toán & tip` vì liên hệ trực tiếp với bài toán ML.

#### F08. Fare actual vs predicted

- Tên file: `fare_actual_vs_predicted.png`
- Vị trí: Phần 5, kết quả Fare.
- Scatter plot có đường `y = x`; nên ghi sample size và metric trong caption.
- Dùng test set thực tế của lần train có metrics được báo cáo.

#### F09. Tip actual vs predicted

- Tên file: `tip_actual_vs_predicted.png`
- Vị trí: Phần 5, kết quả Tip.
- Cùng quy ước với Fare để dễ so sánh hình thức, nhưng không so sánh giá trị
  metric khác đơn vị.

#### F10. Feature importance Fare và Tip

- Tên file: `fare_tip_feature_importance.png`
- Vị trí: Phần 5, phân tích mô hình.
- Có thể dùng hai subplot; lấy importance trực tiếp từ model artifact.
- Không tự gán thứ hạng feature dựa trên kỳ vọng.

### 4.2. Hình nên có

#### F11. Row count qua các lớp

- Tên file: `pipeline_row_counts.png`
- Dạng: funnel hoặc horizontal bars.
- Dữ liệu: Bronze -> Silver Core -> Gold candidate ->
  `gold_fare_tip_features`.
- Chỉ ghép các số nếu chúng thuộc snapshot có lineage tương thích. Nếu không,
  chú thích rõ ngày/artifact của từng số và không dùng funnel hàm ý cùng run.

#### F12. Data quality summary

- Tên file: `data_quality_summary.png`
- Dạng: ma trận/checklist theo layer, số check pass/fail.
- Nguồn: quality reports thực tế.
- Không biến thành biểu đồ trang trí; phải cho thấy check nào bảo vệ invariant
  nào.

#### F13. Phân phối Fare và Tip trước/sau filter

- Tên file: `fare_tip_target_distributions.png`
- Dạng: histogram/boxplot hai target.
- Mục tiêu: giải thích outlier filter và độ khó của Tip.
- Cần ghi rõ dữ liệu sampled hay full.

#### F14. Phân phối payment type

- Tên file: `payment_type_distribution.png`
- Mục tiêu: giải thích vì sao Tip model lọc `payment_type = 1`.
- Caption phải phân biệt tip được ghi nhận trong dữ liệu và tip tiền mặt ngoài
  hệ thống.

#### F15. Residual distribution

- Tên file: `fare_tip_residuals.png`
- Dạng: hai histogram hoặc residual vs predicted.
- Mục tiêu: cho thấy bias, spread và outlier còn lại.

### 4.3. Hình tùy chọn

- `airflow_gold_dag.png`: screenshot Graph view của Gold DAG sau một run thành
  công.
- `minio_medallion_buckets.png`: screenshot MinIO buckets/path, chỉ dùng nếu
  cần bằng chứng triển khai.
- `postgres_schemas_tables.png`: screenshot pgAdmin hoặc sơ đồ schema
  `ml`/`mart`/`staging`/`audit`.
- `fastapi_openapi.png`: screenshot Swagger/OpenAPI cho các endpoint đang chạy.
- `bronze_record_anatomy.pdf`: minh họa raw JSON payload và Kafka metadata.
- `weather_join_cardinality.png`: minh họa one weather row per hour và
  many-to-one join từ taxi trips.

Các screenshot công cụ chỉ nên dùng để chứng minh triển khai. Không nên dùng
quá nhiều screenshot terminal, log hoặc source code vì khó đọc và ít giá trị
phân tích.

## 5. Thứ tự ưu tiên hoàn thiện hình

### Mức P0: cần có trước khi nộp

1. `architecture_overview.pdf`
2. `medallion_data_flow.pdf`
3. `serving_publication_flow.pdf`
4. `streamlit_dashboard_overview.png`
5. `fare_actual_vs_predicted.png`
6. `tip_actual_vs_predicted.png`
7. `fare_tip_feature_importance.png`

### Mức P1: tăng chất lượng báo cáo rõ rệt

1. `single_host_deployment.pdf`
2. `silver_transformation_flow.pdf`
3. `fare_tip_target_distributions.png`
4. `fare_tip_residuals.png`
5. `data_quality_summary.png`

### Mức P2: bằng chứng triển khai bổ sung

1. Airflow DAG screenshot.
2. FastAPI OpenAPI screenshot.
3. PostgreSQL schema screenshot.
4. MinIO bucket screenshot.

## 6. Quy tắc chèn hình vào LaTeX

Mẫu dùng cho hình đơn:

```latex
\begin{figure}[H]
    \centering
    \includegraphics[width=0.92\textwidth]
        {figures/architecture_overview.pdf}
    \caption{Kiến trúc tổng thể của hệ thống MetroPulse}
    \label{fig:architecture-overview}
\end{figure}
```

Quy tắc:

- Mỗi hình phải được nhắc tới trong nội dung bằng
  `Hình~\ref{fig:architecture-overview}`.
- Caption phải nói hình chứng minh điều gì, không chỉ lặp tên hình.
- Sơ đồ nên xuất PDF/SVG để chữ không vỡ.
- Screenshot nên crop phần thừa và che credential/IP nếu xuất hiện.
- Không chụp `.env`, password, token hoặc public VM IP.
- Không dùng hình có số liệu khác với bảng kết quả trong báo cáo.
- Một hình chỉ nên xuất hiện một lần; tránh vẽ lại cùng luồng ở Phần 3 và
  Phần 4.

## 7. Các chỉnh sửa bắt buộc trong bản LaTeX hiện tại

Trước khi hoàn thiện câu chữ, cần sửa các điểm sau:

1. Xóa mọi đề cập Power BI trong các phần 1, 3 và 4.
2. Xóa Transformer và LightGBM khỏi danh sách mô hình đã/định triển khai.
3. Viết lại Phần 5 thành Fare/Tip XGBoost baseline.
4. Sửa Phần 6 để đánh giá metrics Fare/Tip thực tế.
5. Đổi câu hỏi nghiệp vụ chính ở Phần 2 để bao gồm Fare và Tip.
6. Thay toàn bộ khung `\fbox` gợi ý hình bằng `\includegraphics`.
7. Không dùng bảng kết quả có dấu `--`.
8. Kiểm tra lại các số liệu snapshot trước khi khẳng định một luồng lineage
   end-to-end duy nhất.
9. Bổ sung `\label` cho tất cả bảng/hình cần tham chiếu.
10. Bổ sung tài liệu tham khảo hoặc loại bỏ các phát biểu cần citation.

## 8. Nguồn bằng chứng trong repository

Nội dung báo cáo nên được đối chiếu từ:

- Kiến trúc/trạng thái: `README.md`, `PROGRESS.md`, `docker-compose.yml`.
- Bronze/Silver/Gold: `src/processing/`, `src/quality/`.
- Airflow: `dags/`.
- PostgreSQL publication: `src/serving/`, `sql/postgres/`.
- FastAPI: `src/dashboard_api/main.py`.
- Streamlit: `src/dashboard_app/streamlit_app.py`.
- ML config: `ml/configs/xgb_fare_tip.yaml`.
- ML training: `ml/train/fare_tip_model.py`.
- ML feature engineering: `ml/train/fare_tip_feature_engineering.py`.
- ML results: `ml/logs/fare_metrics.json`, `ml/logs/tip_metrics.json`.
- EDA evidence: các notebook trong `notebooks/`.

Nếu một thông tin chỉ xuất hiện trong văn bản báo cáo nhưng không có bằng chứng
trong các nguồn trên hoặc artifact chạy thực tế, cần loại bỏ hoặc chuyển thành
hướng phát triển.
