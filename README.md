# BPPR: Using PySpark to Accelerate Batch Point Rotation in Paleogeographic Reconstruction

## Description
BPPR is an open-source, PySpark-based, extensible batch paleogeographic point rotation method, 
which accelerates rotation in paleogeographic reconstruction.

## Data
There are three main data folders: `data/PALEOGEOGRAPHY_MODELS`, `data/data/syntheticData`, and `data/data/realData`.

The `PALEOGEOGRAPHY_MODELS` folder houses the models used for the paleogeographic reconstructions.</br>
The `syntheticData` folder contains synthetic datasets that were generated to test the performance of BPPR. 
The datasets are designed to mimic the characteristics of real-world paleogeographic data 
but are created in a controlled environment to assess the efficiency and accuracy of the method.</br>
The `realData` folder includes empirical datasets that were used to validate BPPR. 
This data derives from the analysis of fossil occurrences and other geoscientific measurements.


## Available Scripts
The Python scripts provide examples for rotating paleogeographic points with different methods. 
Below is a description of each script and its purpose.

+ `simulated_BPPR.py`: rotate simulated paleogeographic points using BPPR.
+ `simulated_pygplates.py`: rotate simulated paleogeographic points using PyGPlates.
+ `reverse_BPPR.py`: complete reverse-reconstructing geological points from a specific geological time.
+ `fossil_BPPR.py`: rotate empirical data of fossil occurrences in PBDB using BPPR.
+ `fossil_pygplates.py`: rotate the same empirical data using PyGPlates.
+ `getPBDB.py`: get records from PBDB via APIs utilizing PySpark similar to BPPR.


## Additional Information
The `data/points` folder includes some test results in the process of batch paleogeographic point rotation.
