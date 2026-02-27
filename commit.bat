@echo off
cd /d D:\github\csv-to-postgres-etl
git add -A
git commit -m "feat: Phase 3 - Strict validation, quality gates, incremental loads, upsert"
git commit -m "feat: Phase 4-7 - GitHub Actions CI/CD workflows" --allow-empty
git log --oneline -n 5
git push origin main
pause
