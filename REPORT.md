# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Lê Tuấn Hưng` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602665` |
| GitHub | `Justoroamming` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02134` |

## Kết quả cuối

- `dataset_version` của bạn: `f8ba96293523`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `738 / 139 / 142`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

**Trả lời:**
4 khác biệt chính giữa telemetry A và B:
1. **Tên cột**: A dùng `ts_utc`, `speed_mps`, `heading_deg`, `gas`, `brake`, `steer`; B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `throttle_pct`, `brake_pct`, `steering`
2. **Đơn vị tốc độ**: A dùng m/s (1.213), B dùng km/h (55.11)
3. **Kiểu timestamp**: A dùng ISO UTC string (2025-10-07T08:00:00.014Z), B dùng epoch ms (1759933801509)
4. **Đồng hồ thiết bị**: B bị lệch 1.5 giây so với video (cần trừ clock offset), A thì khớp video

Pipeline giữ mỗi nguồn một bảng bronze riêng vì schema drift (cấu trúc khác nhau) - gộp ngay sẽ mất thông tin gốc, khó debug khi có lỗi. Giữ chữ (string) cho mọi cột ở bronze đảm bảo không mất dữ liệu khi ép kiểu sai (null, giá trị vô lý), việc chuẩn hóa kiểu dữ liệu, đơn vị, đồng hồ để sau ở Silver khi đã hiểu rõ schema từng nguồn.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Trả lời:**
Pipeline chuẩn hóa qua dbt staging models:
- **Đơn vị**: B chuyển km/h → m/s (chia 3.6)
- **Kiểu thời gian**: B chuyển epoch ms → UTC timestamp, rồi trừ clock_offset 1.5s (khai báo trong sources.json)
- **Kết quả**: B bắt đầu lúc `2025-10-08 14:30:00.009` khớp video start

**Bảng quarantine:**
| drive_id | reject_reason | so_dong |
|----------|---------------|---------|
| drive_a  | duplicate     | 20      |
| drive_a  | speed_missing | 30      |
| drive_a  | speed_out_of_range | 3 |
| drive_b  | duplicate     | 20      |
| drive_b  | speed_missing | 30      |
| drive_b  | speed_out_of_range | 3 |

Tổng 106 dòng bị cách ly (53 mỗi drive). Dòng lỗi vào `rejects` kèm lý do thay vì xoá im lặng để: (1) có thể kiểm tra/audit lại, (2) đếm số lượng từng loại lỗi, (3) không mất dữ liệu gốc - bronze vẫn giữ nguyên, (4) tuân thủ nguyên tắc "không xóa dữ liệu" của data lakehouse.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

**Trả lời:**
- `max_gap_ms` là **warn** vì gap lớn chỉ báo hiệu cảm biến mất tín hiệu tạm thời (có thể do môi trường), không nhất thiết là dữ liệu sai - pipeline vẫn chạy tiếp để không chặn toàn bộ.
- `in_range` là **error** vì tốc độ ngoài phạm vi vật lý (0-70 m/s) là dữ liệu chắc chắn sai, phải chặn để không lan truyền.

**Thí nghiệm break-gate**: Nới max_speed từ 70 → 1000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` **FAIL** bắt được **6 dòng** (3 mỗi drive). Điều này cho thấy cổng chất lượng (quality gate) hoạt động: warn chỉ báo, error chặn dữ liệu xấu đi tiếp. Khi luật được nới, test error phát hiện ngay dữ liệu bất thường.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

**Trả lời:**
- Trước: 1246 dòng, fingerprint `6c7480dfde7e`
- Sau 2 lần chạy lại: 1246 dòng, fingerprint `6c7480dfde7e` (**không đổi**)
- Ghi kiểu `append` sai: 60 khung → 120 dòng (**nhân đôi**)

Delta `overwrite` theo partition (trip_id) an toàn vì: (1) chỉ ghi đè partition đó, partition khác không đụng, (2) nguyên tử - hoặc thành công hết hoặc rollback, không có trạng thái trung gian mất dữ liệu, (3) nếu lỗi giữa chừng, partition cũ vẫn nguyên vẹn (time travel), (4) idempotent - chạy lại bao nhiêu lần kết quả y hệt.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

**Trả lời:**
- **Khung đen**: camera che khuất/ngủ, pixel toàn 0 - pipeline **loại bỏ** (đối tượng `is_black=true`)
- **Khung tối**: đường hầm/bóng râm, sáng yếu nhưng vẫn có nội dung - pipeline **gắn cờ `is_low_light` nhưng GIỮ LẠI** (drive B: 61 khung tối giữ lại)
- Tôi **đồng ý** vì khung tối vẫn chứa thông tin hữu ích (đèn xe, biển báo), chỉ cần model xử lý được low-light.

**Chọn ngưỡng 2** (ngưỡng hiện tại): Cân bằng tốt - loại được khung đứng hình (camera frozen 6s ở drive A) nhưng không loại khung khác nhau thật. Ngưỡng 0 quá chặt (chỉ 61/8), ngưỡng 6 quá lỏng (363/244 - loại cả khung khác nhau). Phụ thuộc tốc độ: xe chạy chậm → ít thay đổi giữa các khung → dHash nhỏ → cần ngưỡng thấp hơn; xe chạy nhanh → khung thay đổi nhiều → ngưỡng có thể cao hơn.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

**Trả lời:**
- Chia theo **khung hình** (ngẫu nhiên 70/15/15): **26/26 trip** bị chia cắt (tất cả trip đều có khung ở nhiều split), **26 trip** có khung ở cả train và test
- Chia theo **trip** (pipeline): **0 trip** bị chia cắt

**Tại sao giả tạo**: Các khung liên tiếp trong cùng trip gần như giống nhau (động học liên tục). Nếu chia ngẫu nhiên khung, nửa đầu trip ở train, nửa sau ở test → mô hình "đã thấy" đề thi khi test (data leakage) → accuracy cao giả tạo, không phản ánh khả năng generalize thực tế.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

**Trả lời:**
- `dataset_version` = SHA256(nội dung dataset) nên: (1) **bất biến** - cùng input → cùng version, (2) **có thể verify** - ai cũng tính được lại, (3) **phát hiện thay đổi** - 1 bit khác → version khác hoàn toàn.
- 2 lần `export-dataset` liên tiếp: **cùng `f8ba96293523`** (manifest_sha256 cũng giống).
- Khác bạn bè vì **MSSV (2A202602665)** được hash vào việc xếp trip vào train/val/test (stratified shuffle với seed = MSSV). MSSV khác → thứ tự trip khác → split khác → nội dung dataset khác → version khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

**Trả lời:**
Từ `manifest.json` lineage:
```json
"lineage": {
  "bronze.raw_telemetry_a": 0,
  "bronze.raw_telemetry_b": 0,
  "bronze.trips_raw": 0,
  "bronze.video_files": 1,
  "silver.frames": 25,
  "silver.telemetry": 0,
  "silver.frames_flagged": 0,
  "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc"
  }
}
```
Dataset được tạo từ: bronze video_files (v1), silver frames (v25), silver telemetry/frames_flagged/frame_telemetry (v0), bronze telemetry/trips (v0), video drive_a/b với SHA256 như trên.

3 tháng sau tái tạo cần: (1) **MSSV** (để split trip giống hệt), (2) **video SHA256** (đảm bảo video gốc không đổi), (3) **các bảng Delta version** (lineage), (4) **dataset_version** để verify kết quả.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

**Trả lời:**
- **1 task** chuyển đỏ: `silver_frames` map_index=5 (trip-02134) - log: `simulated crash while extracting trip-02134`
- **7 task** `upstream_failed`: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`
- **Clear**: Chỉ clear task đỏ `silver_frames[5]` → nó chạy lại thành công → các task downstream tự chạy tiếp → DAG xanh toàn bộ
- **Dữ liệu**: **Không nhân đôi** (idempotent), verify 27/27 passed
- **dataset_version**: **Không đổi** (`f8ba96293523`)
- Bằng chứng: Log task đỏ thấy `RuntimeError: simulated crash while extracting trip-02134`; verify output 27/27; dataset_version giống trước/sau.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

**Trả lời:**
1. **Video decode (FFmpeg)** - Slide 22/24: CPU bound, single-thread per video. 500 xe × 5p/ngày = 2500 video/ngày. Thay bằng: **GPU decode (NVDEC/VideoToolbox)** + **distributed worker pool** (Kubernetes/Ray)
2. **Airflow LocalExecutor** - Slide 26/27: chạy tuần tự trên 1 máy, 26 task song song max. Thay bằng: **CeleryExecutor/KubernetesExecutor** + **dynamic task mapping** scale theo số trip
3. **DuckDB single-node** - Slide 12/22: in-process, không scale. Thay bằng: **ClickHouse/Trino** cluster cho OLAP, hoặc **Spark/Databricks** cho ELT distributed

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Trả lời:**
```sql
SELECT 
  drive_id,
  trip_id,
  COUNT(*) as frames,
  AVG(speed_kmh) as avg_speed_kmh,
  MAX(speed_kmh) as max_speed_kmh,
  SUM(CASE WHEN is_low_light THEN 1 ELSE 0 END) as low_light_frames
FROM marts.mart_frame_gallery
GROUP BY drive_id, trip_id
ORDER BY drive_id, trip_id;
```
Câu hỏi này cho thấy: **Phân bố khung hình và tốc độ trung bình/tối đa theo từng trip**, cùng số khung tối (low-light) - giúp nhanh chóng nhận biết trip nào dữ liệu ít, trip nào nhiều khung tối cần review thêm, hỗ trợ quyết định augment data hay collect thêm.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

**Trả lời:**
- Bất ngờ: Delta Lake overwrite partition thực sự idempotent - chạy lại 100 lần vẫn 1246 dòng, fingerprint không đổi.
- Chưa rõ: Làm sao tune ngưỡng dHash tự động theo tốc độ xe thay vì fix cứng 2 bit.
- Lỗi chưa bắt: **Telemetry order đảo ngược** (timestamp giảm dần) - pipeline chỉ check `max_gap_ms` warn, không chặn. Cần test `ts_utc` monotonic increasing.