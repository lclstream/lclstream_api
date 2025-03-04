# Workflows

## OM workflow

- A set of detectors (specified by a data source string) are each configured using parameters given in a config. file 
  - these parameters allow adding important metadata not present in event data (e.g. [detector distance of Jungfrau 1M](https://github.com/omdevteam/om/blob/f0bd85b4f70730d44cbd696f8c1c0fb1aa81bffe/src/om/data_retrieval_layer/data_retrieval_zmq.py#L65))

- Each data source has a setup function, run at the start of taking data, and a process function run on each event

- Users will often process all frames in SMD mode, and then later re-processes events, reading only the hits (IDX mode)


## TMO workflow

- The TMO beamline processes psana2 data by building an [event pipeline](https://github.com/lcls-users/tmo-prefex/blob/david-dev/tmo_prefex/cmd/xtc2h5.py).
  - Each detector has a [configuration block](https://github.com/lcls-users/tmo-prefex/blob/david-dev/config.yaml)
  - A per-detector setup() function checks this configuration against psana2's view of the experiment.
  - A pipeline of events goes through extraction (get data from psana), processing (do any compression/event detection), and pooling steps (combine multiple events to a block of events).

- SMD mode is used exclusively, but events can be filtered based on per-detector configuration (e.g. thresholding).

- The output h5 file contains arrays of "event number" and "data from events" for one single run, grouped by step and then by detector name, then channel

## ML training workflow

- A set of cleaned data files (e.g. zarr) form a training dataset.  These have been gathered from some local processing on S3DF.

- When used for ML model training, a set of producers work on one file each, reading and sending an image at a time.

- The state of the "read" can be tracked using a linear index through each file.
  - Note if we want to mix up the order for ML training, this index may map to some permutation of the source frames.

- An intermediate "cache" may be used to pool reads over multiple file-readers and send to multiple consumers.


## Notes

- parallel psana1 and psana2 (SMD) data are not guaranteed to be chronological
  - idx not supported in psana2

- psana1 and psana2 do not support "resume" directly. We need to manually skip forward to a point.

- Do users re-process a dataset? Using IDX or SMD?
