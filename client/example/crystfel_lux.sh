#!/usr/bin/env bash
# Job body: open a transfer, run CrystFEL against it, ship the stream.
#
#   crystfel_lux.sh <exp=...,run=...> <token path> [indexamajig args...]
#
# The caller's arguments replace the defaults below. We still own the
# input URI, geometry, and output paths, which only exist here.
#
# Two images: ours only talks to lclstream_api, CrystFEL's is upstream and
# unmodified, so its arguments and geometry change without a rebuild.
set -uo pipefail

# Slurm starts us in the job dir, which the submit script sets.
JOB_DIR=$PWD

# Slurm inherits nothing, so the xrootd settings come from here.
set -a
[[ -f .env ]] && . ./.env
set +a

export LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER=${1:?"expected exp=<exp>,run=<run>"}
TOKEN_FILE=${2:?"expected the staged token path"}
shift 2
# Defaults only; the caller's string replaces all of them.
CRYSTFEL_ARGS=("$@")
(( $# )) || CRYSTFEL_ARGS=(
    --zmq-request=next
    --data-format=msgpack
    --peaks=msgpack
    --copy-header=timestamp
    --copy-header=event_id
    --copy-header=source
)
[[ -r "${TOKEN_FILE}" ]] || { echo "[token] not readable: ${TOKEN_FILE}"; exit 1; }
echo "[source] ${LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER}"

XRD_DEST=${XRD_DEST:-}
export XrdSecSSSKT=${XrdSecSSSKT:-${HOME}/.xrd/sss_lstream.keytab}
XRD_URL=${XRD_URL:-root://sdfdtn006.slac.stanford.edu:3091}
export XRD_NETWORKSTACK=IPv4

# Lux compute nodes have no direct outbound access, so the client's HTTPS
# calls to lclstream_api go through OLCF's proxy; apptainer passes these
# into the container. The proxy only carries HTTP(S), not ZMQ's raw TCP.
export http_proxy=${http_proxy:-http://proxy.ccs.ornl.gov:3128/}
export https_proxy=${https_proxy:-http://proxy.ccs.ornl.gov:3128/}
export no_proxy=${no_proxy:-localhost,127.0.0.0/8,*.ccs.ornl.gov}

# Lustre, not NFS home: indexamajig's temp files and the stream land here.
SCRATCH_DIR=${SCRATCH_DIR:-/lustre/orion/csc741/scratch/${USER}/crystfel_demo}
# exp=<exp>,run=<run> -> "<exp>_<run>", so a stream says which data it holds
# and the job id keeps two runs of one exp/run apart.
SOURCE_SLUG=${LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER#exp=}
SOURCE_SLUG=${SOURCE_SLUG/,run=/_}
STREAM_NAME=crystfel.${SOURCE_SLUG}.${SLURM_JOB_ID:-manual}.stream
STREAM=${SCRATCH_DIR}/${STREAM_NAME}
mkdir -p "${SCRATCH_DIR}" || exit 1

echo "[crystfel] args: ${CRYSTFEL_ARGS[*]}"

CLIENT_IMAGE=${CLIENT_IMAGE:-/ccs/home/vmarslac/apptainer/lclstream-client.sif}
CRYSTFEL_IMAGE=${CRYSTFEL_IMAGE:-/ccs/home/vmarslac/apptainer/crystfel.sif}

client() {
    # Not --env: it splits on commas, which cuts exp=...,run=... at the run.
    APPTAINERENV_LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER="${LCLSTREAM_CRYSTFEL_SOURCE_IDENTIFIER}" \
    apptainer run \
        --bind "${JOB_DIR}:/cfg:ro" \
        --bind "${TOKEN_FILE}:/run/s3df_token:ro" \
        "${CLIENT_IMAGE}" "$@"
}

# Slurm SIGTERMs us at the wall clock, and bash skips EXIT traps on an
# untrapped signal, so catch both or a timeout leaks the transfer.
cleanup() {
    trap - EXIT TERM INT
    [[ -n "${CRYSTFEL_PID:-}" ]] && kill "${CRYSTFEL_PID}" 2>/dev/null
    [[ -n "${TRANSFER_ID:-}" ]] && client cancel "${TRANSFER_ID}"
    rm -f "${TOKEN_FILE}"
}
trap cleanup EXIT
trap 'cleanup; exit 143' TERM INT

opened=$(client create | tee /dev/stderr) || { echo "[transfer] create failed"; exit 1; }
TRANSFER_ID=$(sed -n 's/^TRANSFER_ID=//p' <<<"${opened}")
URI=$(sed -n 's/^URI=//p' <<<"${opened}")
[[ -n "${TRANSFER_ID}" && -n "${URI}" ]] || { echo "[transfer] no URI"; exit 1; }

# Backgrounded on purpose: bash defers traps while a foreground child
# runs, so a wall-clock SIGTERM would never reach cleanup.
rc=0
apptainer exec \
    --bind "${JOB_DIR}:/cfg:ro" \
    --bind "${SCRATCH_DIR}:/out" \
    "${CRYSTFEL_IMAGE}" \
    indexamajig \
        --zmq-input="${URI}" \
        "${CRYSTFEL_ARGS[@]}" \
        --temp-dir=/out \
        -o "/out/${STREAM_NAME}" &
CRYSTFEL_PID=$!
wait "${CRYSTFEL_PID}" || rc=$?
CRYSTFEL_PID=
echo "[crystfel] exit ${rc}"

# Ship whatever indexed, even on failure. Lux has no xrdcp yet, so a
# missing client or keytab keeps the stream here rather than failing.
if [[ -z "${XRD_DEST}" ]]; then
    echo "[xrootd] XRD_DEST unset; stream kept at ${STREAM}"
elif [[ ! -s "${STREAM}" ]]; then
    echo "[xrootd] no stream to copy"
elif ! command -v xrdcp >/dev/null; then
    echo "[xrootd] no xrdcp on this host; stream kept at ${STREAM}"
elif [[ ! -r "${XrdSecSSSKT}" ]]; then
    echo "[xrootd] no keytab: ${XrdSecSSSKT}; stream kept at ${STREAM}"
else
    dest="${XRD_URL}/${XRD_DEST%/}/${STREAM_NAME}"
    echo "[xrootd] copying ${STREAM} to ${dest}"
    xrdcp --force "${STREAM}" "${dest}" || rc=$?
fi

echo "[done] exit ${rc}"
exit "${rc}"
