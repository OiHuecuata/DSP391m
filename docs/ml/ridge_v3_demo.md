# V3 Ridge demonstration

This is a local run of the repository's Spark 4.0.2 / Delta 4.0.1 provincial Ridge pipeline, using the attached Dataset_v3.zip. It does not replace the live S3 dataset or the V1 readiness audit.

V3's manifest explicitly declares 649 original posts and 5,841 simulated posts, with synthetic publication context. Results are demonstrations, not independently validated market-demand forecasts.

The unchanged eligibility and temporal configuration admits 16 provinces. Training uses eight weekly lags and predicts one week ahead. The last complete observation week ends 25 October 2026; the forecast is for the historical week starting 26 October 2026, not the current week.

The three walk-forward test folds cover 464 province-weeks. Spark produced MAE 0.4010 versus moving-average MAE 0.4219, and RMSE 2.0068 versus 2.0691. Some evaluation fits emitted Cholesky/L-BFGS warnings. Independent NumPy Ridge comparison found a maximum test prediction difference of 0.4712, so the website labels evaluation results provisional. Do not use those scores to claim verified model accuracy.

The separately fitted final model's predictions matched the independent centered SSE + alpha*L2 solution within 8.95e-11. This validates numerical agreement for this demo fit; it does not establish real-world forecasting performance.

Files:
- ridge_v3_demo.json: aggregated website forecast and held-out chart data only.
- ridge_v3_training.json: configuration, coverage and recorded evaluation results.
- ridge_v3_verification.json: independent numerical comparison, including the failed strict fold parity check.

Full model artifacts are saved locally at /home/coara/Documents/Codex/ridge-v3-demo/staging/provincial_ridge/runs/v3-demo-20261112. No model files or account information were uploaded to S3.

To reproduce in an environment with the repository's locked Spark/Delta versions and requirements-ridge.txt:

```bash
PYTHONPATH=pipelines/spark/src python scripts/website/train_ridge_demo.py \
  --dataset /path/to/Dataset_v3.zip \
  --workdir /path/to/new/local/demo-directory \
  --run-id unique-v3-demo-run
```

The exporter writes local artifacts only. Review the run and its solver diagnostics before replacing the website JSON. It preserves the mixed-data disclosure and provisional evaluation label.
