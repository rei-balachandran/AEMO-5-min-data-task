# AEMO-5-min-data-task
This task download a set of zip files from https://nemweb.com.au/Reports/Archive/DispatchIS_Reports/
from a set period, the zip files are extracted into csv files, 5 minutes interval energy rate of NSW, 
QLD, TAS, VIC and SA are extracted, and converted to 30 minutes resolution. The 30 minutes resolution 
is then plotted in a graph.

## To run the script

### To clean up data
This script download files into resource folder, before running the script, clean the resource folder using the following script
Make sure the working directory are in the `AEMO-5-min-data-task` working directory.

```
./cleanup.sh
```

### To run the Python script
1. Install Python version 3.9
2. Create a virtual environment
3. Activate the virtual environment
4. Install the libraries mentioned in requirements.txt

```
pip install -r requirements.txt
```
5. Ensure you are in the the `AEMO-5-min-data-task` working directory, and run the following:

```
python src/extract_prices_and_plot.py --start_date 20221201 --end_date 20231101 --num_threads 30
```
6. The visualisation of each state will be in the `plot` directory

**Note**: A`duplicate_check.py` module has also been included to check for duplicate data from 
the interval csv data. However, the output of this script did not indicate any instances of 
duplicate values in any of the interval csf data.
