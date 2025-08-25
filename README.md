# QMS — Lite Weighted DRP/MRP System

> A pragmatic, data‑driven supply planning toolkit for demand classification, smart lead times, safety stock, ETA prediction, and replenishment (MRP / DRP). Designed for messy ERP data and real‑world processes.

---

## ✨ Key Features
- **Demand Classification** — steady / intermittent / burst / trended / seasonal, with replacement & stock‑only handling.
- **Forecasting** — Croston and other strategies via a registry; writes unified forecast artifacts.
- **Safety Stock** — pluggable strategy registry; service‑level driven, robust to outliers.
- **Smart Lead Time** — merges vendor transport stats, preferences, and prepare time with layered fallbacks.
- **ETA Transformer** — five path splits (confirmed / shipped / tail-batch / batch‑prone / single‑batch), virtual sub‑lines, overdue logic.
- **Transport Mode Intelligence** — correct / predict transport mode with group penalties and static caps.
- **Route Selector** — candidate (vendor + mode) selection with cost/lead‑time tradeoffs; supports quotes and standard cost.
- **Freight Charge Analyzer** — currency normalization, sanity checks, and cost-per‑kg modeling (e.g., regression with non‑negative constraints).
- **MRP / DRP** — vectorized standard MRP; output includes algorithm tag & transport mode; dynamic MRP planned.

> Built around functional, composable components, with optional OOP facades for pipelines/jobs.

---

## 🧠 Core Concepts

### Demand Classification
- Handles **replaced items**, **stock‑only**, **new items**, and main classes: steady / intermittent / burst / trended / seasonal.
- Winsorization, weighted stats, decomposition for trend/seasonality.

### Forecasting
- Strategy registry dispatches by demand type (e.g., Croston variants for intermittent).
- Unified outputs (series, monthly aggregates, model_used) stored via `ItemForecastRecord`.

### Safety Stock
- Pluggable strategies (e.g., service‑level, variability‑aware) under `forecast/safety_stock/`.

### Smart Lead Time
- Combines **TransportPreference**, **VendorTransportStats**, **PrepareLTStats**, and static caps.
- Lower/upper bounds = *min across stats and static*, robust fallbacks, no `999` magic numbers.

### ETA Transformer
- Five flow types; supports **virtual sub‑lines**, tail‑batch simulation, and overdue re‑assessment (Q60→Q90 fallback).

### Transport Mode Intelligence
- **CorrectTransportMode / PredictTransportMode** with group‑switch penalties; bans intra‑group flips.

### Route Selector
- Candidate routes assembled from historical preference + vendor stats + quotes.
- Strategy: **lowest total cost** (unit price + freight), or shortest lead time. Prepare time integration is planned.

### Freight Charge Analyzer
- Ingests `PO_FREIGHT_CHARGE` & `MultiCurrency`; monthly FX; non‑negative regression to split base & per‑kg costs.

### MRP / DRP
- **Static MRP** implemented; writes algorithm tag and transport mode to `MRP_ORDERS`.
- **Dynamic MRP** planned to incorporate dynamic lead times.

---

## 🛠 Usage Examples
> Examples assume you’ve configured DB connections and have access to the required tables.

```python
from qms_core.forecast.classifier import DemandClassifier
from qms_core.mrp.calculator import MRPCalculator

item = Item(ITEMNUM="ABC", Warehouse="TOKYO")
DemandClassifier().classify(item)
mrp = MRPCalculator(algorithm="Static")
orders = mrp.run(item)
```

### CLI / Jobs (illustrative)
building

---

## 🗃️ Data Inputs (typical)
- `DemandHistoryWeekly`, `ItemMaster (IIM)`, `Inventory`, `VendorMaster`
- `VendorTransportStats`, `ItemTransportPreference`, `ItemPrepareLTStats`
- `PO_DeliveryHistoryRaw`, `PO_FREIGHT_CHARGE`, `MultiCurrency`

---

## 🔌 Extensibility
- **Strategy Registries** — add new forecasting or safety‑stock strategies without touching callers.
- **Analyzers** — drop‑in analyzers (e.g., freight, delivery behavior) with common base class.
- **Pipelines/Jobs** — ETL‑style `prepare → process → write` lifecycle with dry‑run & smart writer support.

---

## 📈 Roadmap
- Dynamic MRP with path‑aware ETA
- Prepare‑time integration in RouteSelector
- Cost + lead‑time multi‑objective path selection
- CI workflow & data‑contract checks

---

## 🤝 Contributing
1. Fork + branch: `feat/*` or `fix/*`
2. Format & lint: `ruff`, `black`
3. Add tests where meaningful
4. PR template & checklist

---

## 🪪 License
TBD (MIT recommended). If you need dual‑license or exceptions for client data, open an issue first.

---

## ⚠️ Security
- Never commit credentials or raw client data.
- Use `.env` and secrets managers; rotate keys on any suspicion.
- If sensitive files were accidentally committed, remove with `git rm --cached` and rotate secrets; for full history rewrite consider `git filter-repo`/BFG.

---

## 🙋 FAQ
**Q:** Can I run QMS without a live ERP?  
**A:** Yes, by pointing to local CSV/SQLite mirrors and stubbing extractors.

**Q:** Does it support partial batches and tail‑batch ETAs?  
**A:** Yes — see ETATransformer’s tail‑batch simulation and overdue heuristics.

---

## 🧭 Credits
Designed for messy, real‑world supply chains. Built with love for functional composition and clean, testable code.

