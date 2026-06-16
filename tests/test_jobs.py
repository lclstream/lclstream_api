import psik

from lclstream_api.lclstreamer_param import Parameters
from lclstream_api.jobs import (
    create_job,
    has_cache,
    replay_job,
    generate_job,
    get_outdir,
)

from test_config import config

param1 = """{
  "lclstreamer": {
    "source_identifier": "exp1,run2",
    "event_source": "Psana1EventSource",
    "processing_pipeline": "BatchProcessingPipeline",
    "data_serializer": "Hdf5BinarySerializer",
    "data_handlers": ["BinaryDataStreamingDataHandler"],
    "skip_incomplete_events": false
  },
  "event_source": {"Psana1EventSource":{}},
  "data_sources": {},
  "data_serializer": {
    "Hdf5BinarySerializer": {
      "fields": { }
    }
  },
  "data_handlers": {
    "BinaryDataStreamingDataHandler": {
      "urls": []
    }
  },
  "processing_pipeline": {
    "BatchProcessingPipeline": {
      "batch_size": 10
    }
  }
}
"""

param2 = """{
  "lclstreamer": {
    "source_identifier": "none",
    "event_source": "InternalEventSource",
    "processing_pipeline": "BatchProcessingPipeline",
    "data_serializer": "Hdf5BinarySerializer",
    "data_handlers": ["BinaryDataStreamingDataHandler"],
    "skip_incomplete_events": false
  },
  "event_source": {
    "InternalEventSource": {
      "number_of_events_to_generate": 1001
    }
  },
  "data_sources": {
    "random": {
      "type": "GenericRandomNumpyArray",
      "array_shape": [20, 2],
      "array_dtype": "float32"
    }
  },
  "data_serializer": {
    "Hdf5BinarySerializer": {
      "compression_level": 3,
      "compression": "zfp",
      "fields": {
        "random": "/data/random"
      }
    }
  },
  "data_handlers": {
    "BinaryDataStreamingDataHandler": {
      "urls": []
    }
  },
  "processing_pipeline": {
    "BatchProcessingPipeline": {
      "batch_size": 10
    }
  }
}
"""


def test_outdir(config):
    req1 = Parameters.model_validate_json(param1)
    req2 = Parameters.model_validate_json(param2)

    out1 = get_outdir(req1, config)
    out2 = get_outdir(req2, config)
    out3 = get_outdir(req1, config)
    assert out1 == out3
    assert out1 != out2


def test_create(config):
    req = Parameters.model_validate_json(param1)
    spec = create_job(req, "tcp://127.0.0.1:5001", config)
    assert isinstance(spec, psik.JobSpec)


def test_generate(config):
    req = Parameters.model_validate_json(param1)
    spec = generate_job(req, "tcp://127.0.0.1:5001", config)
    assert isinstance(spec, psik.JobSpec)


def test_replay(config, tmpdir):
    spec = replay_job(tmpdir / "cache", "tcp://127.0.0.1:5001", config)
    assert isinstance(spec, psik.JobSpec)
