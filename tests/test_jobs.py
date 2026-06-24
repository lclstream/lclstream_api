import psik

from lclstream_api.jobs import (
    create_producer,
    create_forwarder,
    generate_job,
    get_outdir,
    replay_job,
)
from lclstream_api.models import PortEntry

from lclstream_api.lclstreamer_param import Parameters

from test_config import config

param1 = """{
  "source_identifier": "exp1:run2",
  "skip_incomplete_events": false,
  "event_source": {
      "type": "Psana1EventSource"
  },
  "data_sources": {},
  "processing_pipeline": {
    "type": "BatchProcessingPipeline",
    "batch_size": 10
  },
  "data_serializer": {
    "type": "HDF5BinarySerializer",
    "fields": {}
  },
  "data_handlers": [
    {"type": "BinaryDataStreamingDataHandler",
     "urls": []
    }
  ]
}
"""

param2 = """{
  "source_identifier": "none",
  "skip_incomplete_events": false,
  "event_source": {
    "type": "InternalEventSource",
    "number_of_events_to_generate": 1001
  },
  "data_sources": {
    "random": {
      "type": "GenericRandomNumpyArray",
      "array_shape": [20, 2],
      "array_dtype": "float32"
    }
  },
  "processing_pipeline": {
    "type": "BatchProcessingPipeline",
    "batch_size": 10
  },
  "data_serializer": {
    "type": "HDF5BinarySerializer",
    "compression_level": 3,
    "compression": "zfp",
    "fields": {
        "random": "/data/random"
    }
  },
  "data_handlers": [
    { "type": "BinaryDataStreamingDataHandler",
      "urls": []
    }
  ]
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


def test_create_producer(config):
    req = Parameters.model_validate_json(param1)
    spec = create_producer(req, "tcp://127.0.0.1:5001", config)
    assert isinstance(spec, psik.JobSpec)

def test_create_forwarder(config):
    entry = PortEntry(eid=1, user="tester", port=10000, internal_url="tcp://a:10000", external_url="tcp://a:10001")
    spec = create_forwarder(entry, config)
    assert isinstance(spec, psik.JobSpec)

def test_generate(config):
    req = Parameters.model_validate_json(param1)
    spec = generate_job(req, "tcp://127.0.0.1:5001", config)
    assert isinstance(spec, psik.JobSpec)


def test_replay(config, tmpdir):
    spec = replay_job(tmpdir / "cache", "tcp://127.0.0.1:5001", config)
    assert isinstance(spec, psik.JobSpec)
