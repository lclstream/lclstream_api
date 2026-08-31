"""TODO: mirrors lclstream_api.lclstreamer_param again, need a central place to put this..."""

from pathlib import Path
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ._generated.models.parameters import Parameters as _GeneratedParameters


class _CustomBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


####### Event Sources ########


class InternalEventSourceParameters(_CustomBaseModel):
    """Configuration parameters for the Internal Event Source."""

    type: Literal["InternalEventSource"]
    number_of_events_to_generate: int
    model_config = ConfigDict(extra="allow")


class Psana1EventSourceParameters(_CustomBaseModel):
    """Configuration parameters for the Psana1 Event Source."""

    type: Literal["Psana1EventSource"]


class Psana2EventSourceParameters(_CustomBaseModel):
    """Configuration parameters for the Psana2 Event Source."""

    type: Literal["Psana2EventSource"]


EventSourceParameters = Annotated[
    InternalEventSourceParameters
    | Psana1EventSourceParameters
    | Psana2EventSourceParameters,
    Field(discriminator="type"),
]


###### Data Sources #######


class GenericRandomNumpyArrayParameters(_CustomBaseModel):
    """Parameters for the GenericRandomNumpyArray class."""

    type: Literal["GenericRandomNumpyArray"]
    array_shape: int | tuple[int, ...]
    array_dtype: str
    always_random: bool = True

    @field_validator("array_shape", mode="before")
    @classmethod
    def convert_int_to_tuple(cls, v):
        if isinstance(v, str):
            parts = [p.strip() for p in v.split(",") if p.strip()]
            return tuple(int(p) for p in parts)
        if isinstance(v, int):
            return (v,)
        return v


class ConstValueParameters(_CustomBaseModel):
    """Parameters for ConstValue class."""

    type: Literal["ConstValue"]
    value: int | float | list[int | float]
    dtype: str

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, v: Any):
        if isinstance(v, str):  # "6," -> [6]
            parts = [p.strip() for p in v.split(",") if p.strip()]
            if len(parts) == 1:
                return int(parts[0]) if parts[0].isdigit() else float(parts[0])
            return [int(p) if p.isdigit() else float(p) for p in parts]
        return v


class _PsanaDetectorInterfaceParameters(_CustomBaseModel):
    psana_name: str
    psana_fields: list[str] | str | None = None

    @model_validator(mode="after")
    def validate_fields(self):
        if ":" not in self.psana_name and self.psana_fields is None:
            raise ValueError(
                "psana_fields must be specified when psana_name is not a PV."
            )
        return self


class Psana1DetectorInterfaceParameters(_PsanaDetectorInterfaceParameters):
    type: Literal["Psana1DetectorInterface"]


class Psana2DetectorInterfaceParameters(_PsanaDetectorInterfaceParameters):
    type: Literal["Psana2DetectorInterface"]
    dtype: str | None = None


class Psana2TimestampParameters(_CustomBaseModel):
    """Parameters for psana2 timestamp interface."""

    type: Literal["Psana2Timestamp"]


class Psana1TimestampParameters(_CustomBaseModel):
    """Parameters for psana1 timestamp interface."""

    type: Literal["Psana1Timestamp"]


class SourceIdentifierParameters(_CustomBaseModel):
    """Parameters for source identifier data source interface."""

    type: Literal["SourceIdentifier"]


class Psana2RunInfoParameters(_CustomBaseModel):
    """Parameters for run info data source interface."""

    type: Literal["Psana2RunInfo"]


DataSourceParameters = Annotated[
    GenericRandomNumpyArrayParameters
    | ConstValueParameters
    | Psana1DetectorInterfaceParameters
    | Psana2DetectorInterfaceParameters
    | Psana1TimestampParameters
    | Psana2TimestampParameters
    | SourceIdentifierParameters
    | Psana2RunInfoParameters,
    Field(discriminator="type"),
]


####### Processing Pipelines #########


class BatchProcessingPipelineParameters(_CustomBaseModel):
    """Configuration parameters for the Batch Processing Pipeline."""

    type: Literal["BatchProcessingPipeline"]
    batch_size: int


class PeaknetPreprocessingPipelineParameters(_CustomBaseModel):
    """Configuration parameters for the PeakNet Preprocessing Pipeline."""

    type: Literal["PeaknetPreprocessingPipeline"]
    batch_size: int
    target_height: int
    target_width: int
    pad_style: Literal["center", "bottom-right"] = "center"
    add_channel_dim: bool = True
    num_channels: int = 1


ProcessingPipelineParameters = Annotated[
    BatchProcessingPipelineParameters | PeaknetPreprocessingPipelineParameters,
    Field(discriminator="type"),
]


####### Serializers ##########


class SimplonBinarySerializerParameters(_CustomBaseModel):
    """Configuration parameters for the Simplon binary serializer."""

    type: Literal["SimplonBinarySerializer"]
    data_source_to_serialize: str
    polarization_fraction: float
    polarization_axis: list[float]
    data_collection_rate: str
    detector_name: str
    detector_type: str


class HDF5BinarySerializerParameters(_CustomBaseModel):
    """Configuration parameters for the HDF5 binary serializer."""

    type: Literal["HDF5BinarySerializer"]
    compression_level: int = 0
    compression: (
        Literal[
            "gzip",
            "gzip_with_shuffle",
            "bitshuffle_with_lz4",
            "bitshuffle_with_zstd",
            "zfp",
        ]
        | None
    ) = None
    fields: dict[str, str]


DataSerializerParameters = Annotated[
    HDF5BinarySerializerParameters | SimplonBinarySerializerParameters,
    Field(discriminator="type"),
]


######### Data Handlers #################


class BinaryDataStreamingDataHandlerParameters(_CustomBaseModel):
    """Configuration parameters for the Binary Data Streaming Data Handler."""

    type: Literal["BinaryDataStreamingDataHandler"]
    urls: list[str]
    distribute: bool = True
    buffer: int = 0
    role: Literal["server", "client"] = "client"
    library: Literal["zmq"] = "zmq"
    socket_type: Literal["push"] = "push"


class BinaryFileWritingDataHandlerParameters(_CustomBaseModel):
    """Configuration parameters for the Binary File Writing Data Handler."""

    type: Literal["BinaryFileWritingDataHandler"]
    file_prefix: str = ""
    file_suffix: str = "h5"
    write_directory: Path = Field(default_factory=Path.cwd)


DataHandlerParameters = Annotated[
    BinaryDataStreamingDataHandlerParameters | BinaryFileWritingDataHandlerParameters,
    Field(discriminator="type"),
]


class Parameters(_CustomBaseModel):
    """Top-level configuration parameters for an lclstreamer run."""

    source_identifier: str
    skip_incomplete_events: bool

    event_source: EventSourceParameters
    data_sources: dict[str, DataSourceParameters]
    processing_pipeline: ProcessingPipelineParameters
    data_serializer: DataSerializerParameters
    data_handlers: list[DataHandlerParameters]

    @model_validator(mode="after")
    def _check_model(self) -> Self:
        if self.data_serializer.type == "SimplonBinarySerializer":
            required_sources = [
                "timestamp",
                "detector_data",
                "detector_geometry",
                "run_info",
            ]
            source_missing = [
                k for k in required_sources if k not in self.data_sources.keys()
            ]
            if source_missing:
                raise ValueError(
                    f"Required fields: {source_missing} is missing from data_sources "
                    "for SimplonBinarySerializer."
                )

        return self

    def to_generated(self) -> _GeneratedParameters:
        """Convert to the generated ``_generated.models.Parameters``."""
        generated = _GeneratedParameters.from_dict(self.model_dump(mode="json"))
        assert generated is not None
        return generated
