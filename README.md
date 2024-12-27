# Stock Analyzer
- python version: 3.12
- macbook air, m1 chip
## Environment Setup

### mysql setup
Install mysql
```shell
brew install mysql
```
Start mysql
```shell
brew services start mysql
```
mysql setup
```mysql
CREATE DATABASE stock_data;

USE stock_data;

CREATE TABLE stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,      -- 自動遞增的主鍵
    stock_no VARCHAR(10) NOT NULL,          -- 股票代碼
    date DATE NOT NULL,                     -- 交易日期
    volume BIGINT,                          -- 成交股數
    turnover BIGINT,                        -- 成交金額
    open_price DECIMAL(10, 2),              -- 開盤價
    high_price DECIMAL(10, 2),              -- 最高價
    low_price DECIMAL(10, 2),               -- 最低價
    close_price DECIMAL(10, 2),             -- 收盤價
    price_change VARCHAR(20),               -- 漲跌價差
    transaction_count INT                   -- 成交筆數
);
```
### Python Setup
Install prerequisite
```shell

```

## Getting Start