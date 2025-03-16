
library(sparklyr)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)

# Cấu hình Spark với các tùy chọn xử lý datetime và memory
config <- spark_config()
config$spark.sql.legacy.timeParserPolicy <- "LEGACY"  
config$`sparklyr.shell.driver-memory` <- "4g"
config$`sparklyr.shell.executor-memory` <- "4g"
config$spark.sql.session.timeZone <- "UTC"
config$spark.sql.shuffle.partitions <- "200"  

# Khởi tạo kết nối Spark với cấu hình tùy chỉnh
sc <- spark_connect(master = "local", config = config, version = "3.4.0")

# Đọc dữ liệu từ CSV vào Spark, giữ InvoiceDate là string
data <- spark_read_csv(sc, 
                       name = "online_retail",
                       path = "D:/Big Data/BTL/OnlineRetail.csv",
                       header = TRUE,
                       columns = list(
                         InvoiceNo = "character",
                         StockCode = "character",
                         Description = "character",
                         Quantity = "integer",
                         InvoiceDate = "character",
                         UnitPrice = "double",
                         CustomerID = "integer",
                         Country = "character"
                       ),
                       charset = "ISO-8859-1")

# Kiểm tra cấu trúc dữ liệu
data %>% head() %>% collect()

# -------------------- Làm sạch Dữ liệu --------------------
# Loại bỏ các dòng có CustomerID bị thiếu, có số lượng âm và UnitPrice âm
data_cleaned <- data %>%
  filter(!is.na(CustomerID) & Quantity > 0 & UnitPrice > 0)

# Chuẩn hóa định dạng ngày tháng và chuyển đổi sang kiểu datetime
data_cleaned <- data_cleaned %>%
  mutate(
    InvoiceDate = regexp_replace(InvoiceDate, "^(\\d{1})-(\\d{1})-(\\d{4})", "0$1-0$2-$3"),  # Thêm số 0 vào ngày và tháng nếu cần
    InvoiceDate = regexp_replace(InvoiceDate, "^(\\d{2})-(\\d{1})-(\\d{4})", "$1-0$2-$3"),   # Thêm số 0 vào tháng nếu cần
    InvoiceDate = regexp_replace(InvoiceDate, "^(\\d{1})-(\\d{2})-(\\d{4})", "0$1-$2-$3"),   # Thêm số 0 vào ngày nếu cần
    InvoiceDateTime = to_timestamp(InvoiceDate, "dd-MM-yyyy HH:mm")  # Chuyển đổi sang datetime
  )

# Cache data_cleaned để tối ưu hiệu suất (nếu dữ liệu lớn)
data_cleaned <- data_cleaned %>% sdf_persist(storage.level = "MEMORY_AND_DISK")

# Kiểm tra lại dữ liệu đã làm sạch
data_cleaned %>% head() %>% collect()

# -------------------- 1. Doanh thu theo Quốc gia --------------------
sales_by_country <- data_cleaned %>%
  group_by(Country) %>%
  summarise(TotalSales = sum(Quantity * UnitPrice)) %>%
  arrange(desc(TotalSales)) %>%
  collect()

# Vẽ biểu đồ doanh thu theo quốc gia
ggplot(head(sales_by_country, 10), aes(x = reorder(Country, TotalSales), y = TotalSales)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  coord_flip() +
  labs(title = "Doanh thu theo Quốc gia", x = "Quốc gia", y = "Doanh thu") +
  theme_minimal()

# -------------------- 2. Top Sản phẩm bán chạy --------------------
top_products <- data_cleaned %>%
  group_by(Description) %>%
  summarise(TotalQuantity = sum(Quantity)) %>%
  arrange(desc(TotalQuantity)) %>%
  collect()

# Vẽ biểu đồ sản phẩm bán chạy
ggplot(head(top_products, 10), aes(x = reorder(Description, TotalQuantity), y = TotalQuantity)) +
  geom_bar(stat = "identity", fill = "coral") +
  coord_flip() +
  labs(title = "Top 10 Sản phẩm bán chạy nhất", x = "Sản phẩm", y = "Số lượng bán") +
  theme_minimal()

# -------------------- 3. Doanh thu theo Tháng --------------------
monthly_sales <- data_cleaned %>%
  mutate(Month = date_format(InvoiceDateTime, "yyyy-MM")) %>%
  group_by(Month) %>%
  summarise(TotalSales = sum(Quantity * UnitPrice)) %>%
  collect()

# Vẽ biểu đồ doanh thu theo tháng
ggplot(monthly_sales, aes(x = Month, y = TotalSales)) +
  geom_line(color = "blue") +
  labs(title = "Doanh thu theo Tháng", x = "Tháng", y = "Doanh thu") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))

# -------------------- 4. Phân khúc khách hàng theo chi tiêu --------------------
customer_spending <- data_cleaned %>%
  group_by(CustomerID) %>%
  summarise(TotalSpending = sum(Quantity * UnitPrice)) %>%
  collect() %>%
  mutate(Segment = cut(TotalSpending,
                       breaks = c(-Inf, 50, 100, 500, Inf),
                       labels = c("Thấp", "Trung Bình", "Cao", "Rất Cao")))

# Vẽ biểu đồ phân khúc khách hàng
ggplot(customer_spending, aes(x = Segment)) +
  geom_bar(fill = "purple") +
  labs(title = "Phân khúc khách hàng theo chi tiêu", x = "Phân khúc", y = "Số lượng khách hàng") +
  theme_minimal()

# -------------------- 5. Tần suất giao dịch của khách hàng --------------------
transaction_frequency <- data_cleaned %>%
  group_by(CustomerID) %>%
  summarise(Transactions = n_distinct(InvoiceNo)) %>%
  collect()

# Vẽ biểu đồ tần suất giao dịch của khách hàng
ggplot(transaction_frequency, aes(x = Transactions)) +
  geom_histogram(binwidth = 1, fill = "green", color = "black") +
  labs(title = "Tần suất giao dịch của khách hàng", x = "Số lần giao dịch", y = "Số lượng khách hàng") +
  theme_minimal()

# -------------------- 6. Doanh thu theo ngày trong tuần --------------------
weekday_sales <- data_cleaned %>%
  mutate(Weekday = date_format(InvoiceDateTime, "EEEE")) %>%
  group_by(Weekday) %>%
  summarise(TotalSales = sum(Quantity * UnitPrice)) %>%
  collect()

# Vẽ biểu đồ doanh thu theo ngày trong tuần
ggplot(weekday_sales, aes(x = reorder(Weekday, TotalSales), y = TotalSales)) +
  geom_bar(stat = "identity", fill = "cyan") +
  coord_flip() +
  labs(title = "Doanh thu theo Ngày trong Tuần", x = "Ngày trong tuần", y = "Doanh thu") +
  theme_minimal()

# -------------------- 7. Sản phẩm theo mức giá --------------------
products_by_price <- data_cleaned %>%
  group_by(Description, UnitPrice) %>%
  summarise(TotalQuantity = sum(Quantity)) %>%
  arrange(UnitPrice) %>%
  collect()

# Vẽ biểu đồ sản phẩm theo mức giá
ggplot(products_by_price, aes(x = UnitPrice, y = TotalQuantity)) +
  geom_point(color = "blue") +
  labs(title = "Sản phẩm theo mức giá", x = "Giá đơn vị", y = "Số lượng bán") +
  theme_minimal()

# -------------------- Lưu kết quả phân tích --------------------
# Tạo thư mục output nếu chưa tồn tại
output_dir <- "D:/output/"
dir.create(output_dir, showWarnings = FALSE)

# Lưu các dataframe thành CSV
write.csv(sales_by_country, file = paste0(output_dir, "sales_by_country.csv"), row.names = FALSE)
write.csv(top_products, file = paste0(output_dir, "top_products.csv"), row.names = FALSE)
write.csv(monthly_sales, file = paste0(output_dir, "monthly_sales.csv"), row.names = FALSE)
write.csv(customer_spending, file = paste0(output_dir, "customer_spending.csv"), row.names = FALSE)
write.csv(transaction_frequency, file = paste0(output_dir, "transaction_frequency.csv"), row.names = FALSE)
write.csv(weekday_sales, file = paste0(output_dir, "weekday_sales.csv"), row.names = FALSE)
write.csv(products_by_price, file = paste0(output_dir, "products_by_price.csv"), row.names = FALSE)

# Ngắt kết nối Spark
# spark_disconnect(sc)

# Thông báo hoàn thành
cat("Phân tích hoàn tất. Kết quả đã được lưu trong thư mục", output_dir)
