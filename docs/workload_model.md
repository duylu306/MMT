# Mô Hình Tải Thực Nghiệm (`workload_model.md`)

Tài liệu này định nghĩa chính xác về mặt toán học và kỹ thuật cho bộ công cụ sinh tải (`loadgen/`) mô phỏng hành vi máy chủ biên (edge gateway) gửi dữ liệu tồn đọng về trung tâm Apache IoTDB sau sự cố gián đoạn/chập chờn mạng.

---

## 1. Mô hình Dữ liệu Logic và Tính Bất Biến theo Khối Lượng

### 1.1. Schema Thiết Bị và Chuỗi Thời Gian
- **Database đích:** `root.bench`
- **Số luồng máy biên gửi đồng thời (`num_clients`):** $C \in \{1, 4, 16\}$.
- Mỗi luồng client $c \in \{0, 1, \dots, C - 1\}$ mở một phiên `iotdb.Session.Session` riêng biệt tới DataNode và ghi độc lập vào thiết bị riêng của biên đó: `root.bench.edge_{c}`.
- Mỗi thiết bị có $M = 6$ cảm biến kiểu số thực 64-bit (`TSDataType.DOUBLE`): `s0, s1, s2, s3, s4, s5` (mô phỏng các đại lượng đo vật lý như nhiệt độ, áp suất, dòng điện, điện áp, độ rung, công suất).
- **Kích thước dữ liệu logic mỗi bản ghi (row):**
  $$b_{\text{row}} = \underbrace{8\text{ bytes}}_{\text{INT64 timestamp}} + \underbrace{6 \times 8\text{ bytes}}_{6 \times \text{DOUBLE values}} = 56\text{ bytes/record}$$

### 1.2. Bảo Toàn Tổng Khối Lượng Dữ Liệu Logic
- Với tổng số bản ghi mục tiêu $N_{\text{total}}$ của một lần chạy, mỗi client $c$ phụ trách chính xác:
  $$N_c = \left\lfloor \frac{N_{\text{total}}}{C} \right\rfloor + \mathbb{I}\big(c < (N_{\text{total}} \bmod C)\big)$$
  sao cho $\sum_{c=0}^{C-1} N_c = N_{\text{total}}$ và tổng dung lượng dữ liệu logic $B_{\text{logical}} = N_{\text{total}} \times 56\text{ bytes}$ cố định tuyệt đối giữa các cấu hình so sánh.
- Đối với các ô đặc biệt có chi phí mỗi lệnh RPC quá cao (ví dụ `batch_size = 1`), nếu dùng $N_{\text{total}}$ rút gọn có kiểm soát thì mọi chỉ số chi phí ghi (Write Amplification, byte ghi/bản ghi, CPU ms/bản ghi) đều được chuẩn hóa theo khối lượng dữ liệu logic $B_{\text{logical}}$ và $N_{\text{total}}$.

---

## 2. Cơ Chế Sinh Dữ Liệu Tái Lập Theo Seed (`seed`)

Để đảm bảo **"cùng `seed` cho cùng chuỗi timestamp"** và tách biệt hoàn toàn giữa *tập dữ liệu logic gốc* với *nhiễu trễ mạng*, mỗi client $c$ khởi tạo ba bộ sinh số giả ngẫu nhiên độc lập (`numpy.random.Generator` PCG64) dẫn xuất từ `SeedSequence([seed, c, stream_id])`:

1. **`STREAM_BASE = 0` (Dữ liệu cảm biến & chuỗi timestamp gốc):**
   - Sinh chuỗi thời gian phát sinh chuẩn tắc tăng nghiêm ngặt với chu kỳ lấy mẫu $\Delta t = 10\text{ ms}$ bắt đầu từ mốc cố định $t_0 = 1\,700\,000\,000\,000\text{ ms}$ (`2023-11-14T22:13:20Z`):
     $$t_{c,i} = t_0 + i \cdot \Delta t, \quad i = 0, 1, \dots, N_c - 1$$
   - Sinh ma trận giá trị đo vật lý có tương quan chuỗi thời gian (tín hiệu tuần hoàn cộng nhiễu Gauss) $\mathbf{V}_c \in \mathbb{R}^{N_c \times M}$ chỉ phụ thuộc vào `(seed, c)`.
   - Hệ quả: **Tập hợp các bộ dữ liệu logic $\{(t_{c,i}, \mathbf{v}_{c,i})\}_{i=0}^{N_c-1}$ hoàn toàn bất biến** khi thay đổi `ooo_ratio`, `delay_dist`, `delay_mean` hoặc `batch_size` (nên kích thước dữ liệu nén lý tưởng sau khi hội tụ là như nhau).

2. **`STREAM_SELECT = 1` (Chọn tập bản ghi đến muộn):** Chọn tập chỉ số bản ghi bị trễ theo `ooo_ratio`.
3. **`STREAM_DELAY = 2` (Sinh độ lớn độ trễ):** Sinh biến ngẫu nhiên độ trễ $D_{c,i} > 0$ theo `delay_dist` và `delay_mean`.

---

## 3. Định Nghĩa Chính Xác `ooo_ratio` và Phân Bố Độ Trễ

### 3.1. Định nghĩa tỷ lệ sai thứ tự mức luồng đến ($r_{\text{empirical}}$)
Giả sử các bản ghi của một thiết bị đến máy chủ theo trình tự chỉ số nhận $k = 0, 1, \dots, N_c - 1$ với chuỗi nhãn thời gian tương ứng $(\tau_0, \tau_1, \dots, \tau_{N_c - 1})$. Đặt mốc thời gian lớn nhất đã từng ghi nhận trước vị trí $k$:
$$M_{k-1} = \max_{0 \le j < k} \tau_j \quad (\text{quy ước } M_{-1} = -\infty)$$

Một bản ghi tại vị trí $k$ được gọi là **đến sai thứ tự (out-of-order)** nếu nhãn thời gian của nó nhỏ hơn mốc cực đại đã thấy trước đó ($\tau_k < M_{k-1}$). Tỷ lệ sai thứ tự thực nghiệm của luồng dữ liệu được tính bằng:
$$r_{\text{empirical}} = \frac{1}{N_c} \sum_{k=0}^{N_c - 1} \mathbb{I}(\tau_k < M_{k-1}) \in [0, 1)$$

### 3.2. Thuật toán điều khiển `ooo_ratio` $\in [0\%, 100\%]$
Cho tham số mục tiêu $p = \text{ooo\_ratio} \in [0.0, 1.0]$:

- **Trường hợp $p = 0.0$ (`0%` — Đúng thứ tự hoàn toàn):**
  - Không bản ghi nào bị trễ ($D_i = 0, \forall i$). Luồng gửi đi giữ nguyên thứ tự gốc $\tau_k = t_k$, suy ra $\tau_k > M_{k-1}$ với mọi $k$ và $r_{\text{empirical}} = 0.0$.

- **Trường hợp $0.0 < p < 1.0$ (Mô hình trễ mạng từng phần):**
  - Chọn ngẫu nhiên không hoàn lại chính xác $K = \text{round}(p \cdot N_c)$ chỉ số bản ghi trong tập $\{0, 1, \dots, N_c - 2\}$ (giữ bản ghi cuối $N_c - 1$ làm mốc neo không trễ để tránh hiệu ứng biên cuối dải hữu hạn).
  - Mỗi bản ghi $i \in \mathcal{S}_{\text{delayed}}$ nhận độ trễ dương $D_i > 0$ lấy mẫu từ phân bố `delay_dist` với kỳ vọng $\mathbb{E}[D] = \text{delay\_mean}$ (đơn vị ms); các bản ghi $j \notin \mathcal{S}_{\text{delayed}}$ có $D_j = 0$.
  - Thời điểm đến trung tâm ảo của bản ghi $i$ là:
    $$a_i = t_i + D_i$$
  - Sắp xếp các bản ghi theo thứ tự tăng dần của $a_i$ (nếu trùng $a_i$ thì phân xử theo chỉ số gốc $i$).
  - **Tính chất toán học quan trọng:**
    - Mọi bản ghi không bị trễ ($D_j = 0 \implies a_j = t_j$) chỉ đứng sau các bản ghi $m$ có $a_m \le t_j \implies t_m \le t_m + D_m \le t_j$, nên **không một bản ghi không trễ nào vi phạm tính đơn điệu**.
    - Mọi bản ghi bị trễ $i \in \mathcal{S}_{\text{delayed}}$ có $D_i$ đủ vượt qua ít nhất một bản ghi không trễ kế tiếp $m > i$ ($t_i + D_i > t_m$) đều chắc chắn thỏa $\tau_k = t_i < t_m \le M_{k-1}$. Với $\Delta t = 10\text{ ms}$ và $\text{delay\_mean} \ge 1\,000\text{ ms}$, xác suất vượt ít nhất 1 mẫu không trễ đạt $> 99.8\%$, nên $r_{\text{empirical}} \approx p$ với sai số tuyệt đối rất nhỏ ($< 0.5\%$).

- **Trường hợp $p = 1.0$ (`100%` — Xáo trộn hoàn toàn):**
  - Thực hiện hoán vị ngẫu nhiên đồng đều toàn bộ $N_c$ bản ghi bằng thuật toán Fisher–Yates (`rng.permutation(N_c)`), phản ánh kịch bản thiết bị biên đổ toàn bộ các khối đệm lưu tạm không theo thứ tự thời gian.
  - Theo lý thuyết tổ hợp, số lượng kỷ lục cực đại chạy (running maxima) trung bình trong một hoán vị ngẫu nhiên cỡ $N_c$ bằng số điều hòa $H_{N_c} = \sum_{j=1}^{N_c} \frac{1}{j} \approx \ln N_c + \gamma$. Với $N_c \ge 5\,000$, $r_{\text{empirical}} \approx 1 - \frac{\ln N_c + 0.577}{N_c} > 99.8\%$.

### 3.3. Ba phân bố độ trễ (`delay_dist`) với cùng kỳ vọng $\mu = \text{delay\_mean}$

Cả ba phân bố đều được tham số hóa sao cho kỳ vọng lý thuyết chính xác bằng $\mathbb{E}[D] = \mu > 0$ (tính bằng mili-giây), nhưng độ biến thiên và độ nặng của đuôi (tail heaviness) tăng dần:

1. **`exponential` (Phân bố Mũ — đuôi nhẹ chuẩn Poisson):**
   $$f(x) = \frac{1}{\mu} e^{-x/\mu} \quad (x > 0), \qquad \mathbb{E}[D] = \mu, \quad \text{CV} = \frac{\sqrt{\text{Var}(D)}}{\mathbb{E}[D]} = 1.0$$

2. **`lognormal` (Phân bố Lôga-chuẩn — đuôi vừa/lệch phải điển hình của trễ mạng WAN):**
   - Cố định tham số hình dạng độ lệch log $\sigma_L = 1.0$, suy ra tham số vị trí $\mu_L = \ln(\mu) - \frac{\sigma_L^2}{2} = \ln(\mu) - 0.5$:
     $$D = \exp(\mu_L + \sigma_L Z), \quad Z \sim \mathcal{N}(0, 1)$$
   - Đảm bảo $\mathbb{E}[D] = \exp(\mu_L + \sigma_L^2 / 2) = \mu$, hệ số biến thiên $\text{CV} = \sqrt{e^{\sigma_L^2} - 1} = \sqrt{e - 1} \approx 1.31$.

3. **`pareto` (Phân bố Pareto Loại II / Lomax — đuôi nặng luật lũy thừa *power-law*):**
   - Dùng phân bố Lomax (Pareto dịch chuyển bắt đầu từ $0$ để có miền giá trị $(0, \infty)$ tương thích tự nhiên với các phân bố trễ khác) với chỉ số đuôi $\alpha = 2.5$ (đuôi nặng: bậc 3 phân kỳ $\mathbb{E}[D^3] = \infty$, phương sai lớn gấp 5 lần phân bố mũ) và hệ số tỷ lệ $s = \mu(\alpha - 1) = 1.5\mu$:
     $$D = s \cdot \left((1 - U)^{-1/\alpha} - 1\right), \quad U \sim \text{Uniform}(0, 1)$$
   - Kỳ vọng lý thuyết: $\mathbb{E}[D] = \frac{s}{\alpha - 1} = \mu$; phương sai $\text{Var}(D) = \frac{\alpha s^2}{(\alpha - 1)^2(\alpha - 2)} = 5\mu^2 \implies \text{CV} = \sqrt{5} \approx 2.236$.

---

## 4. Cơ Chế Gom Lô (`batch_size`) và Tương Tác với Giao Thức IoTDB

Luồng dữ liệu đến $(\tau_k, \mathbf{v}_{\pi(k)})_{k=0}^{N_c-1}$ của mỗi client được cắt thành các lô tuần tự kích thước `batch_size` $= B$:
- **Khi $B = 1$:** Gọi trực tiếp `Session.insert_record(device_id, timestamp, measurements, data_types, values)` cho từng bản ghi đơn lẻ.
- **Khi $B > 1$:** Đóng gói từng đoạn $B$ bản ghi liên tiếp (lô cuối có thể $< B$ nếu $N_c \bmod B \ne 0$) vào `iotdb.utils.NumpyTablet.NumpyTablet` và gọi `Session.insert_tablet(tablet)`.
  - *Đặc điểm kiến trúc quan trọng từ mã nguồn chính thức `apache-iotdb==1.3.3` (`iotdb/utils/NumpyTablet.py`, dòng 56–60):* Hàm khởi tạo `NumpyTablet` kiểm tra thứ tự `timestamps` trong phạm vi lô $B$ và tự động thực hiện `timestamps.argsort()` cục bộ cho $B$ hàng đó trước khi tuần tự hóa nhị phân gửi qua Thrift RPC. Thời gian tạo `NumpyTablet` + gửi RPC đều được đo trọn vẹn trong độ trễ mỗi request của client.

---

## 5. Vòng Đời Chuẩn Hóa Mỗi Lần Chạy (Run Lifecycle)

Để loại bỏ nhiễu chéo giữa các lần chạy:
1. **Warm-up (Khởi động đầu phiên):** Ghi một lượng dữ liệu nháp vào database tạm `root.warmup`, thực thi `FLUSH`, truy vấn và xóa `root.warmup` để kích hoạt biên dịch JIT của JVM trên DataNode và khởi tạo bộ đệm kết nối.
2. **Cleanup & Reset trước mỗi run:**
   - Gửi lệnh xóa sạch database `root.bench` (`delete_storage_group("root.bench")`), sau đó chờ các chỉ số `file_count{name="seq"}` và `file_count{name="unseq"}` của database về `0` (hoặc tạo lại sạch `set_storage_group("root.bench")` kèm đăng ký trước schema timeseries để không lẫn chi phí auto-create schema vào pha đo).
3. **Baseline Snapshot ($t_{\text{start}}$):** Chụp tức thời bộ đếm Prometheus của IoTDB, cAdvisor và `/proc/<datanode_pid>/io` ngay trước khi các luồng client xuất phát đồng bộ qua rào chắn (`threading.Barrier`).
4. **Active Ingestion Window ($[t_{\text{start}}, t_{\text{ingest\_end}}]$):** `num_clients` luồng gửi toàn bộ dữ liệu và ghi nhận cho **mỗi request**: `(request_id, client_id, send_ts_ns, batch_rows, logical_bytes, latency_ms, is_error, error_msg)`.
5. **Flush & Background Compaction Quiesce ($[t_{\text{ingest\_end}}, t_{\text{settle\_end}}]$):**
   - Gửi lệnh `FLUSH root.bench` tường minh để ép phần MemTable cuối cùng còn nằm trong RAM xuống đĩa (đảm bảo 100% dữ liệu logic đã được bền vững hóa thành `TsFile` khi tính Write Amplification, không bị thiên vị cho cấu hình giữ dữ liệu trong RAM).
   - Theo dõi các bộ đếm tác vụ `compaction` đang chạy/xếp hàng qua Prometheus cho tới khi hệ thống hoàn tất đợt gom nền và đi vào trạng thái tĩnh (quiescent) ổn định liên tiếp (hoặc chạm ngưỡng timeout an toàn).
6. **Final Snapshot ($t_{\text{settle\_end}}$) & Log Extraction:** Chụp bộ đếm kết thúc, tính toàn bộ đại lượng chênh lệch ($\Delta$) và trích xuất đoạn nhật ký IoTDB trong khung thời gian $[t_{\text{start}}, t_{\text{settle\_end}}]$ lưu kèm `run_id`.
