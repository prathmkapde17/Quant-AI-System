# Antigravity — Quant Trading System

A production-grade, high-performance quantitative trading system built with Python, TimescaleDB, and React.

## 🚀 System Overview

Antigravity is designed for institutional-level data ingestion, feature engineering, and strategy research. It supports real-time streaming from multiple exchanges, high-speed vectorized backtesting, and a beautiful research dashboard for alpha discovery.

### 🏗️ Architecture

- **Backend**: Python 3.12 (Async/Await)
- **Database**: TimescaleDB (PostgreSQL) for time-series optimization.
- **Cache/Bus**: Redis Streams for real-time data distribution.
- **Frontend**: React 19, Vite, Tailwind CSS v4, Recharts.
- **Containerization**: Docker & Docker Compose.

---

## ✨ Features (Achieved)

### 📈 Phase 1: Data Pipeline & Storage
- **Hybrid Storage**: OHLCV data in TimescaleDB hypertables; raw fallback in Parquet.
- **Exchange Connectors**: Native async connectors for **Binance Futures** and **Angel One (SmartAPI)**.
- **Incremental Ingestion**: Intelligent gap-filling and automatic historical backfilling.

### 🧪 Phase 2: Feature Store & Intelligence
- **Real-time Indicators**: On-the-fly calculation of RSI, EMA, MACD, and Bollinger Bands.
- **Historical Feature Factory**: Massively parallel processing of historical features into the DB.
- **Hypertable Features**: Automated 3-month retention policies for high-speed feature retrieval.

### 🧪 Phase 3: Backtesting & Research Dashboard
- **Vectorized Engine**: Ultra-fast backtesting using NumPy/Pandas matrices (test 50k+ bars in <100ms).
- **Research Dashboard**: A premium, glassmorphism-inspired React dashboard for alpha research.
- **Alpha Analytics**: Real-time calculation of Sharpe Ratio, Max Drawdown, Win Rate, and Total Return.
- **Equity Visualization**: High-fidelity area charts for performance analysis.

### 🛡️ System Hardening
- **Memory Circuit Breaker**: Configurable buffer limits to prevent OOM during DB latency.
- **Resilience**: Exponential backoff reconnection logic for all database operations.
- **Type Safety**: Guaranteed Parquet serialization for long-term data lake stability.

---

## 🛠️ Getting Started

### 1. Prerequisites
- Docker Desktop
- Python 3.12+
- Node.js & NPM

### 2. Infrastructure Setup
```bash
docker-compose up -d
```

### 3. Backend Setup
```bash
pip install -e .
python scripts/setup_db.py
python scripts/run_pipeline.py
```

### 4. Research Dashboard
```bash
cd backtest-ui
npm install
npm run dev
```

---

## 📅 Roadmap
- **Phase 4**: Event-Driven Backtesting (Tick-level fidelity).
- **Phase 5**: Execution Engine & Risk Management.
- **Phase 6**: Live Deployment & Monitoring.
