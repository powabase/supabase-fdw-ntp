# NTP FDW Documentation

Complete documentation for the German NTP Energy Market WASM Foreign Data Wrapper.

## Getting Started

**New to NTP FDW?** Start here:
- 📘 **[Quick Start Guide](../QUICKSTART.md)** - Get up and running in 5 minutes ⭐
- 📖 **[Project Overview](../README.md)** - Features, examples, and architecture
- 🤖 **[AI Assistant Guide](../CLAUDE.md)** - Development guide for Claude Code and AI assistants

## Endpoint Reference

Complete documentation for all 4 foreign tables:

### Energy Production Data
- 📊 **[Renewable Energy Timeseries](renewable-energy.md)**
  Solar, wind onshore, and wind offshore production data (forecast, actual, real-time)
  *9 API endpoints consolidated, 13 columns, ~500ms queries*

### Market & Pricing Data
- 💰 **[Electricity Market Prices](electricity-prices.md)**
  Spot market, premiums, annual values, negative price detection
  *4 API endpoints consolidated, 12 columns, ~200ms queries*

### Grid Operations Data
- ⚡ **[Redispatch Events](redispatch.md)**
  Grid intervention measures and TSO management actions
  *1 API endpoint, 13 columns, ~500ms queries*

- 🚦 **[Grid Status Timeseries](grid-status.md)**
  Minute-by-minute traffic light status (GREEN/YELLOW/RED)
  *1 API endpoint (JSON), 5 columns, 1440 rows/day*

## Technical Documentation

### Architecture & Design
- 🏗️ **[Architecture](../reference/ARCHITECTURE.md)** - Complete design documentation
  *15 Architectural Decision Records (ADRs), validated with 62,500+ rows*

- 🔄 **[ETL Logic](../reference/ETL_LOGIC.md)** - Data transformation details
  *11 transformations: German locale, NULL handling, timestamp normalization*

- 🗺️ **[Query Routing Rules](../reference/ROUTING_RULES.md)** - SQL to API mapping
  *WHERE clause optimization, parameter pushdown, performance strategies*

- 🌐 **[API Specification](../reference/API_SPECIFICATION.md)** - NTP API reference
  *OAuth2 configuration, CSV format specs, 15 endpoints, rate limits*

## Testing & Validation

- 🧪 **[End-to-End Testing Guide](../guides/E2E_TESTING_GUIDE.md)** - Complete testing workflow
  *Local Supabase setup, integration tests, performance benchmarks*

- ⚡ **[Performance Benchmarks](../../tests/test_performance_benchmarks.md)** - Query performance
  *Real-world timing data, scaling characteristics, optimization tips*

## Production Information

- 📦 **[HANDOVER.md](../HANDOVER.md)** - Current status and security fixes
  *v0.2.0 status, 6 critical security fixes (C-1 through C-8), 155 tests passing*

## Quick Links by Task

### For Users
1. **Install and query NTP data** → [QUICKSTART.md](../QUICKSTART.md)
2. **See working examples** → [README.md](../README.md#usage-examples)
3. **Understand table schemas** → [Endpoint docs](#endpoint-reference)
4. **Troubleshoot issues** → Each endpoint doc has troubleshooting section

### For Developers
1. **Build from source** → [README.md](../README.md#building-from-source)
2. **Understand architecture** → [ARCHITECTURE.md](ARCHITECTURE.md)
3. **Add new endpoint** → [CLAUDE.md](../CLAUDE.md#common-development-tasks)
4. **Run tests locally** → [E2E_TESTING_GUIDE.md](E2E_TESTING_GUIDE.md)
5. **Understand transformations** → [ETL_LOGIC.md](ETL_LOGIC.md)

### For AI Assistants
1. **Project overview** → [CLAUDE.md](../CLAUDE.md)
2. **Build commands** → [CLAUDE.md](../CLAUDE.md#quick-reference)
3. **Security fixes** → [CLAUDE.md](../CLAUDE.md#critical-implementation-patterns)
4. **Common tasks** → [CLAUDE.md](../CLAUDE.md#common-development-tasks)

## Documentation Coverage

| Topic | Files | Description |
|-------|-------|-------------|
| **Getting Started** | 2 files | README, QUICKSTART |
| **Endpoint Reference** | 4 files | Complete API reference for all tables |
| **Architecture** | 4 files | Design decisions, ETL, routing, API specs |
| **Testing** | 3 files | E2E guide, benchmarks, validation results |
| **Development** | 1 file | CLAUDE.md (AI assistant guide) |
| **Production** | 1 file | HANDOVER.md (current status) |
| **Total** | 15 files | Complete documentation package |

## Version Information

- **Current Version:** v0.2.0
- **WASM Binary:** 260 KB (optimized, zero WASI CLI imports)
- **Tables:** 4 (renewable energy, electricity prices, redispatch, grid status)
- **API Endpoints:** 15 endpoints consolidated
- **Tests:** 155 passing (100% success rate)
- **Production Ready:** ✅ Yes

## External Resources

- 🌐 **[NTP API](https://www.netztransparenz.de)** - Official German TSO transparency platform
- 🐙 **[GitHub Repository](https://github.com/powabase/supabase-fdw-ntp)** - Source code and releases
- 📦 **[Supabase Wrappers](https://github.com/supabase/wrappers)** - WASM FDW framework
- 🏢 **[Powabase](https://github.com/powabase)** - Renewable energy data platform

---

**Last Updated:** 2025-10-25
**Status:** Production Ready
**Documentation:** Complete
