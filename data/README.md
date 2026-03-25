# Data

`sample_forecast.nc` is the checked-in AtmosLens demo dataset.

It is generated from the Open-Meteo Air Quality API over a small Dublin commuter-belt grid and normalized into an xarray-friendly NetCDF file with:

- `time`, `lat`, `lon` dimensions
- `pm2_5`, `nitrogen_dioxide`, `ozone`, and `european_aqi` variables

Refresh it with:

```bash
.venv/bin/atmoslens-fetch --output data/sample_forecast.nc
```

The fetch path lives in [`src/atmoslens/datasets.py`](../src/atmoslens/datasets.py).

