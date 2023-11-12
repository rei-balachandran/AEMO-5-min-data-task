rm -rf plot/*.png
find resources/clean_data/interval_price/ -maxdepth 1 -name "*.csv" -print0 | xargs -0 rm -f
find resources/raw_data/day_zip/ -maxdepth 1 -name "*.zip" -print0 | xargs -0 rm -f
find resources/raw_data/interval_csv/ -maxdepth 1 -name "*.CSV" -print0 | xargs -0 rm -f
find resources/raw_data/interval_zip/ -maxdepth 1 -name "*.zip" -print0 | xargs -0 rm -f