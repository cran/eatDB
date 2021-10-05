# eatDB 0.5.0

* `dbPull()` now uses `dbSingleDF()` internally, if variables from only a single data table are selected (slightly increases memory usage for these cases but guarantees correct output length and speeds up performance)
* Switched to Github Action for CI

# eatDB 0.4.1

* Fixed bug causing `createDB` to fail when using `"//"` path specifications.

# eatDB 0.4.0

* Initial release on `CRAN`.
